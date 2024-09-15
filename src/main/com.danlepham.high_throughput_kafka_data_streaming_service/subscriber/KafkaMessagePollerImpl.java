package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfigQualifier;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.AbstractDto;
import com.danlepham.high_throughput_kafka_data_streaming_service.exception.ApplicationException;
import com.danlepham.high_throughput_kafka_data_streaming_service.exception.DatabaseException;
import com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor;
import com.danlepham.high_throughput_kafka_data_streaming_service.publisher.KafkaMessagePublisher;
import com.danlepham.high_throughput_kafka_data_streaming_service.publisher.KafkaMessagePublisherQualifier;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.WorkerThreadFactory;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.SmtpEmailService;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.Util;

import io.smallrye.common.constraint.Nullable;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.*;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@ApplicationScoped
@KafkaMessagePollerQualifier
public class KafkaMessagePollerImpl implements KafkaMessagePoller<String, String> {

    private static final Logger log = Logger.getLogger(KafkaMessagePollerImpl.class);
    private static final int MAX_RETRIES = Util.KAFKA_MAX_RETRIES;
    private static final long BACKOFF_DELAY = Util.KAFKA_BACKOFF_DELAY;

    private KafkaListenerContainerRegistry<String, String> listenerContainerRegistry;
    private KafkaConfig kafkaConfig;
    private SmtpEmailService smtpEmailService;
    private KafkaMessagePublisher<AbstractDto, String> kafkaMessagePublisher;

    private final Object mutexShutdown;

    public KafkaMessagePollerImpl() {
        this.mutexShutdown = new Object();
    }

    @Inject
    public KafkaMessagePollerImpl(
            @KafkaListenerContainerRegistryQualifier KafkaListenerContainerRegistry<String, String> listenerContainerRegistry,
            @KafkaConfigQualifier KafkaConfig kafkaConfig,
            SmtpEmailService smtpEmailService,
            @KafkaMessagePublisherQualifier KafkaMessagePublisher<AbstractDto, String> kafkaMessagePublisher) {

        this.listenerContainerRegistry = listenerContainerRegistry;
        this.kafkaConfig = kafkaConfig;
        this.smtpEmailService = smtpEmailService;
        this.kafkaMessagePublisher = kafkaMessagePublisher;
        mutexShutdown = new Object();
    }
    //

    private void backoff() {
        try {
            Thread.sleep(BACKOFF_DELAY);
        } catch (InterruptedException ex) {
            // re-interrupt the current thread to let the thread that's higher up on the callstack catch/handle it
            Thread.currentThread().interrupt();
        }
    }

    public Uni<ScheduledExecutorService> startCommitScheduler(
            final KafkaRebalanceListener rebalanceListener,
            final long commitIntervalMs) {

        return Uni.createFrom().item(() -> {
            final int SCHEDULER_POOL_SIZE = 1;
            ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(
                    SCHEDULER_POOL_SIZE,
                    new WorkerThreadFactory("KAFKA-COMMIT-SCHEDULER"));
            scheduledThreadPool.scheduleAtFixedRate(() -> commitOffsets(rebalanceListener), commitIntervalMs, commitIntervalMs, TimeUnit.MILLISECONDS);

            return scheduledThreadPool;
        });
    }

    /*
        The timeout parameter is the number of milliseconds that the network client inside the kafka consumer will wait for sufficient data to arrive from the network to fill the buffer. If no data is sent to the consumer, the poll() function will take at least this long.
        The poll timeout is the value you pass to the KafkaConsumer poll() method. This is the maximum time the poll() method will block for, after you call it.
        The poll method returns immediately if there are records available. Otherwise, it will await the passed timeout. If the timeout expires, an empty record set will be returned.
    */
    public Uni<Void> poll(final KafkaListenerContainer<String, String> listenerContainer) {
        return Uni.createFrom().item(() -> {
            final KafkaConsumer<String, String> kafkaListener = listenerContainer.getKafkaListener();
            final KafkaRebalanceListener rebalanceListener = listenerContainer.getRebalanceListener();
            final boolean IS_POLLING_ENABLED = listenerContainer.isPollingEnabled();
            final long POLL_TIMEOUT_MS = listenerContainer.getPollTimeoutMs();
            final long POLL_DELAY_MS = listenerContainer.getPollDelayMs();
            final MessageProcessor MESSAGE_PROCESSOR = listenerContainer.getMessageProcessor();
            final boolean COMMIT_IN_BACKGROUND = listenerContainer.isCommitInBackground();

            final int MAX_CONCURRENT_MSG_PROCESSING_TASKS = listenerContainer.getMaxConcurrentMsgProcessors();
            final long COMMIT_INTERVAL_MS = listenerContainer.getCommitIntervalMs();
            final Executor MSG_PROCESS_EXECUTOR =
                    Executors.newFixedThreadPool(MAX_CONCURRENT_MSG_PROCESSING_TASKS, new WorkerThreadFactory("MSG-PROCESSOR"));
            final Collection<CompletableFuture<Void>> consumeMsgTasks = new Stack<>();

            ScheduledExecutorService commitScheduler = null;
            if (COMMIT_IN_BACKGROUND) {
                commitScheduler = startCommitScheduler(rebalanceListener, COMMIT_INTERVAL_MS).await().indefinitely();
            }

            while (IS_POLLING_ENABLED) {
                try {
                    ConsumerRecords<String, String> records = rebalanceListener.poll(POLL_TIMEOUT_MS, POLL_DELAY_MS, COMMIT_IN_BACKGROUND);

                    if (!records.isEmpty()) {
                        final int CURRENT_BATCH_UPPER_BOUND = records.count();

                        for (ConsumerRecord<String, String> record : records) {
                            if (consumeMsgTasks.size() <= CURRENT_BATCH_UPPER_BOUND) {
                                CompletableFuture<Void> consumeMsgTask = CompletableFuture.runAsync(() -> {
                                    consumeMessage(record, rebalanceListener, MESSAGE_PROCESSOR);
                                }, MSG_PROCESS_EXECUTOR);

                                consumeMsgTasks.add(consumeMsgTask);
                            }
                        }

                        CompletableFuture.allOf(consumeMsgTasks.toArray(new CompletableFuture[0]))
                                .join();
                        consumeMsgTasks.clear();

                        if (!COMMIT_IN_BACKGROUND) {
                            commitAsync(kafkaListener, rebalanceListener);
                        }
                    }
                } catch (Throwable t) {
                    if (COMMIT_IN_BACKGROUND && commitScheduler != null) {
                        commitScheduler.shutdown();
                    }
                    applicationShutdown(t,null)
                            .await().indefinitely();
                }
            }

            return null;
        });
    }

    public Uni<Void> applicationStart() {
        return Uni.createFrom().item(() -> {
            try {
                listenerContainerRegistry.initKafkaListenerContainerRegistry()
                        .await().indefinitely();
                listenerContainerRegistry.start()
                        .await().indefinitely();

                List<CompletableFuture<Void>> pollerTasks = new ArrayList<>();
                List<KafkaListenerContainer<String, String>> listenerContainers = listenerContainerRegistry.getKafkaListenerContainers();

                final int NUM_OF_KAFKA_POLLERS = listenerContainers.size();
                final Executor KAFKA_POLLER_EXECUTOR =
                        Executors.newFixedThreadPool(NUM_OF_KAFKA_POLLERS, new WorkerThreadFactory("KAFKA-POLLER"));

                listenerContainers.forEach(listenerContainer -> {
                    CompletableFuture<Void> pollerTask = CompletableFuture.runAsync(() -> poll(listenerContainer).await().indefinitely(), KAFKA_POLLER_EXECUTOR);
                    pollerTasks.add(pollerTask);
                });

                String applicationName = kafkaConfig.getApplicationName();
                smtpEmailService.sendEmailTextAsync(applicationName + " application", applicationName + " has started up successfully with all listeners registered.");

                CompletableFuture.allOf(pollerTasks.toArray(new CompletableFuture[0]))
                        .exceptionally(ex -> {
                            applicationShutdown(ex, "A poller task has failed.")
                                    .await().indefinitely();
                            return null;
                        })
                        .join();

                return null;
            } catch (Throwable t) {
                applicationShutdown(t, "Error occurred during poller initialization phase.")
                        .await().indefinitely();
            }

            return null;
        });
    }

    private void commitOffsets(KafkaRebalanceListener rebalanceListener) {
        try {
            rebalanceListener.commitOffsets(true);
        } catch (Throwable t) {
            String logMsg = "Warning - this commit scheduler experienced a minor hiccup. See stacktrace for details.";
            log.warn(logMsg, t);
            smtpEmailService.sendEmailTextAsync(logMsg, Util.parseExceptionToString(t));
        }
    }

    public Uni<Void> commitSync(final KafkaConsumer<String, String> kafkaListener, final ConsumerRecord<String, String> recordToCommit) {
        return Uni.createFrom().item(() -> {
            try {
                kafkaListener.commitSync(
                        Collections.singletonMap(
                                new TopicPartition(recordToCommit.topic(), recordToCommit.partition()),
                                new OffsetAndMetadata(recordToCommit.offset())
                        )
                );
                log.info("Blocking commit request acknowledged and completed by kafka broker.");
            } catch (Throwable t) {
                onCommitError(t);
            }

            return null;
        });
    }

    public void commitAsync(final KafkaConsumer<String, String> kafkaListener, KafkaRebalanceListener kafkaRebalanceListener) {
        kafkaListener.commitAsync((Map<TopicPartition, OffsetAndMetadata> offsetMap, Exception ex) -> {
            Optional exceptionOpt = Optional.ofNullable(ex);
            if (exceptionOpt.isPresent()) {
                onCommitError(ex);
            } else {
                kafkaRebalanceListener.clearOffsets();
                log.info("Non-blocking commit request acknowledged and completed by kafka broker.");
            }
        });
    }

    private void onCommitError(Throwable t) {
        smtpEmailService.sendEmailTextAsync("Warning - KafkaConsumer missed a commit. See stacktrace for details.", Util.parseExceptionToString(t));
    }

    public void consumeMessage(
            final ConsumerRecord<String, String> message,
            final KafkaRebalanceListener rebalanceListener,
            final MessageProcessor messageProcessor) {

        int retryCount = 0;
        boolean messageProcessed = false;
        while (!messageProcessed) {
            try {
                Optional<String> messageContent = Optional.ofNullable(message.value());

                if (messageContent.isPresent()) {
                    messageProcessor.processMessage(message);
                }
                messageProcessed = true;

            } catch (ApplicationException | DatabaseException | KafkaException e) {
                if (retryCount < MAX_RETRIES) {
                    ++retryCount;
                    backoff();
                } else {
                    applicationShutdown(e, "An application, database or kafka exception has occurred.")
                            .await().indefinitely();
                }
            } catch (Throwable t) {
                kafkaMessagePublisher.publishToDeadLetterQueue(message, message.topic(), t);
                messageProcessed = true;
            } finally {
                if (messageProcessed) {
                    rebalanceListener.stageOffset(message.topic(), message.partition(), message.offset());
                }
            }
        }
    }

    public Uni<Void> applicationShutdown(Throwable t, @Nullable String errorMsg) {
        synchronized (mutexShutdown) {
            return Uni.createFrom().item(() -> {

                String applicationName = kafkaConfig.getApplicationName();
                String emailSubject = errorMsg != null ? errorMsg : "Unknown error occurred.";
                emailSubject += " " + applicationName + " application will now shutdown...";

                log.error(emailSubject, t);
                smtpEmailService.sendEmailTextAsync(emailSubject, Util.parseExceptionToString(t));
                listenerContainerRegistry.stop()
                        .await().indefinitely();

                System.exit(0);
                return null;
            });
        }
    }
}
