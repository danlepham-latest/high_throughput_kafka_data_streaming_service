package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfigQualifier;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.SmtpEmailService;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.Util;
import io.smallrye.mutiny.Uni;
import lombok.NoArgsConstructor;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.CDI;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 * @implNote : if we want parallelism at the registry level as well, then remove @ApplicationScoped for both KafkaListenerContainerRegistryImpl
 * and KafkaMessagePollerImpl to instead allow instantiation of both via new keyword (this will require a singleton ancestor registry
 * that governs descendent registries).
 * For now, one registry and one poller instance is enough to enable parallelism and concurrency at the kafka consumer level for extreme levels of throughput.
 */

@NoArgsConstructor
@ApplicationScoped
@KafkaListenerContainerRegistryQualifier
public class KafkaListenerContainerRegistryImpl implements KafkaListenerContainerRegistry<String, String> {

    private static final Logger log = Logger.getLogger(KafkaListenerContainerRegistryImpl.class);

    private KafkaConfig kafkaConfig;
    private SmtpEmailService smtpEmailService;

    private List<SubscriberGroupConfig> subscriberGroupConfigs;
    private List<KafkaListenerContainer<String, String>> kafkaListenerContainers;

    @Inject
    public KafkaListenerContainerRegistryImpl(
            @KafkaConfigQualifier KafkaConfig kafkaConfig,
            SmtpEmailService smtpEmailService
    ) {
        this.kafkaConfig = kafkaConfig;
        this.smtpEmailService = smtpEmailService;
    }
    //

    public Uni<Void> initKafkaClientConfigs() {
        return Uni.createFrom().item(() -> {
            kafkaConfig.initKerberosEnvironmentVars();
            this.subscriberGroupConfigs = kafkaConfig.buildSubscriberGroupConfigs();

            return null;
        });
    }

    public List<SubscriberGroupConfig> getSubscriberGroupConfigs() {
        return subscriberGroupConfigs;
    }

    public List<KafkaListenerContainer<String, String>> getKafkaListenerContainers() {
        return kafkaListenerContainers;
    }


    /**
        This will retrieve the class loader from the parent module that contains the user's class loaded in memory
        through the thread (which its state contains the class loader of the parent module) that invokes getMessageProcessor
        since multiple projects will re-use this module; otherwise we risk ClassNotFoundException being thrown
        because the incorrect class loader is used.
     */
    public Uni<MessageProcessor> getMessageProcessor(final String FULLY_QUALIFIED_MSG_PROCESSOR_CLASS_NAME) {
        return Uni.createFrom().item(() -> {
            MessageProcessor messageProcessor = null;

            try {
                final ClassLoader cl = Thread.currentThread().getContextClassLoader();
                final Class<?> MSG_PROCESSOR_CLASS = Class.forName(FULLY_QUALIFIED_MSG_PROCESSOR_CLASS_NAME, true, cl);
                final boolean IS_VALID_MSG_PROCESSOR_CLASS = MessageProcessor.class.isAssignableFrom(MSG_PROCESSOR_CLASS);
                if (IS_VALID_MSG_PROCESSOR_CLASS) {
                    messageProcessor = (MessageProcessor) CDI.current().select(MSG_PROCESSOR_CLASS).get();
                } else {
                    throw new ClassNotFoundException("Invalid message processor was supplied.");
                }
            } catch (ClassNotFoundException e) {
                String errorMsg = "Please ensure that you are supplying a fully qualified class name and that it has implemented the com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor interface.";
                log.error(errorMsg, e);
                String applicationName = kafkaConfig.getApplicationName();
                smtpEmailService.sendEmailTextAsync(applicationName + "- Invalid message processor was supplied. App will now shutdown...", errorMsg);
                System.exit(0);
            }

            return messageProcessor;
        });
    }

    public Uni<Void> initKafkaListenerContainerRegistry() {
        return Uni.createFrom().item(() -> {
            initKafkaClientConfigs()
                    .await().indefinitely();

            List<SubscriberGroupConfig> subscriberGroupConfigs = getSubscriberGroupConfigs();

            List<KafkaListenerContainer<String, String>> kafkaListenerContainers = new ArrayList<>();

            subscriberGroupConfigs.forEach(subscriberGroupConfig -> {
                final boolean IS_POLLING_ENABLED = subscriberGroupConfig.isPollingEnabled();
                final int NUM_OF_SUBSCRIBERS = subscriberGroupConfig.getNumOfSubscribers();
                // determine the message processor to use based on subscriberGroupConfig (this message processor will be a stateless singleton that is shared across all the consumers in the group)
                final String FULLY_QUALIFIED_MSG_PROCESSOR_CLASS_NAME = subscriberGroupConfig.getMessageProcessor();
                final MessageProcessor MESSAGE_PROCESSOR = getMessageProcessor(FULLY_QUALIFIED_MSG_PROCESSOR_CLASS_NAME)
                        .await().indefinitely();

                if (IS_POLLING_ENABLED) {
                    for (int subscriberCount = 0; subscriberCount < NUM_OF_SUBSCRIBERS; subscriberCount++) {
                        KafkaListenerContainer<String, String> kafkaListenerContainer =
                                createKafkaListenerContainer(subscriberGroupConfig, MESSAGE_PROCESSOR).await().indefinitely();

                        kafkaListenerContainers.add(kafkaListenerContainer);
                    }
                }
            });

            this.kafkaListenerContainers = kafkaListenerContainers;

            return null;
        });
    }

    public Uni<KafkaListenerContainer<String, String>> createKafkaListenerContainer(
            SubscriberGroupConfig subscriberGroupConfig, MessageProcessor messageProcessor) {
        return Uni.createFrom().item(() ->
                new KafkaListenerContainer<>(subscriberGroupConfig, messageProcessor)
        );
    }

    public Uni<Void> start() {
        return Uni.createFrom().item(() -> {
            List<CompletableFuture<Boolean>> startMsgListenerTasks = new ArrayList<>();
            kafkaListenerContainers.forEach(kafkaListenerContainer -> {
                CompletableFuture<Boolean> startMsgListenerTask =
                        kafkaListenerContainer
                                .start()
                                .subscribeAsCompletionStage();

                startMsgListenerTasks.add(startMsgListenerTask);
            });

            awaitMsgListenerTasks(startMsgListenerTasks);

            return null;
        });
    }

    public Uni<Void> stop() {
        return Uni.createFrom().item(() -> {
            List<CompletableFuture<Boolean>> stopMsgListenerTasks = new ArrayList<>();

            kafkaListenerContainers.forEach(kafkaListenerContainer -> {
                CompletableFuture<Boolean> stopListenerTask =
                        kafkaListenerContainer
                                .stop()
                                .subscribeAsCompletionStage();

                stopMsgListenerTasks.add(stopListenerTask);
            });

            awaitMsgListenerTasks(stopMsgListenerTasks);

            return null;
        });
    }

    public void awaitMsgListenerTasks(List<CompletableFuture<Boolean>> msgListenerTasks) {
        CompletableFuture.allOf(msgListenerTasks.toArray(new CompletableFuture[0]))
                .exceptionally(ex -> {
                    String errorMsg = "This listener task has failed";
                    log.error(errorMsg, ex);

                    smtpEmailService.sendEmailTextAsync(errorMsg, Util.parseExceptionToString(ex));
                    return null;
                })
                .join();

        Map<Boolean, List<CompletableFuture>> taskResults =
                msgListenerTasks.stream().collect(Collectors.partitioningBy(CompletableFuture::isCompletedExceptionally));

        List<CompletableFuture> failedTasks = taskResults.get(true);
        int totalFailedTasks = failedTasks.size();

        if (totalFailedTasks > 0) {
            System.exit(0);
        }
    }
}

