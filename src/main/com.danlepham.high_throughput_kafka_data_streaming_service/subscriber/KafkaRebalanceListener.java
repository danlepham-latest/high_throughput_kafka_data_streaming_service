package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public class KafkaRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger log = Logger.getLogger(KafkaRebalanceListener.class);

    private final KafkaConsumer<String, String> KAFKA_LISTENER;
    private final ConcurrentMap<TopicPartition, OffsetAndMetadata> STAGED_OFFSETS;
    private final Map<TopicPartition, OffsetAndMetadata> COMMIT_OFFSETS;

    KafkaRebalanceListener(final KafkaConsumer KAFKA_LISTENER) {
        this.KAFKA_LISTENER = KAFKA_LISTENER;
        this.STAGED_OFFSETS = new ConcurrentHashMap<>();
        this.COMMIT_OFFSETS = new HashMap<>();
    }
    //

    ConsumerRecords<String, String> poll(long pollTimeoutMs, long pollDelayMs, boolean shouldSynchronize) {
        Duration pollTimeoutDurationMs = Duration.ofMillis(pollTimeoutMs);
        if (shouldSynchronize) {
            try {
                Thread.sleep(pollDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            synchronized (KAFKA_LISTENER) {
                return KAFKA_LISTENER.poll(pollTimeoutDurationMs);
            }
        } else {
            return KAFKA_LISTENER.poll(pollTimeoutDurationMs);
        }
    }

    /**
     * If the hashed TopicPartition key already exists, then insert it anyway because we always want the most recently processed offset from that TopicPartition to commit.
     * Do not use computeIfAbsent nor putIfAbsent as it will not override the value if the key already exists.
     */
    void stageOffset(String topicName, int partition, long offset) {
        STAGED_OFFSETS.put(
                new TopicPartition(topicName, partition),
                new OffsetAndMetadata(offset, "Offset has been processed and is staged for committing.")
        );
    }

    void commitOffsets(boolean shouldSynchronize) {
        if (!STAGED_OFFSETS.isEmpty()) {
            Map<TopicPartition, OffsetAndMetadata> COMMIT_OFFSETS = unstageOffsets();
            int totalStaged = COMMIT_OFFSETS.size();
            if (shouldSynchronize) {
                synchronized (KAFKA_LISTENER) {
                    KAFKA_LISTENER.commitSync(COMMIT_OFFSETS);
                }
            } else {
                KAFKA_LISTENER.commitSync(COMMIT_OFFSETS);
            }
            COMMIT_OFFSETS.clear();
            log.info(totalStaged + " staged offset(s) committed successfully.");
        } else {
            log.info("0 offsets staged for committing. moving on...");
        }
    }

    void clearOffsets() {
        STAGED_OFFSETS.clear();
    }

    /**
     * Retrieve a snapshot of the currently staged offsets, and remove them one by one, adding each one to COMMIT_OFFSETS
     */
    private Map<TopicPartition, OffsetAndMetadata> unstageOffsets() {
        final Map<TopicPartition, OffsetAndMetadata> COMMIT_OFFSETS = this.COMMIT_OFFSETS;
        Iterator<Map.Entry<TopicPartition, OffsetAndMetadata>> offsetItr = STAGED_OFFSETS.entrySet().iterator();

        int totalRemoved = 0;
        while (offsetItr.hasNext()) {
            Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry = offsetItr.next();
            COMMIT_OFFSETS.put(offsetEntry.getKey(), offsetEntry.getValue());
            offsetItr.remove();
            ++totalRemoved;
        }

        log.info(totalRemoved + " offset(s) have been removed from staging and will be committed shortly...");
        return COMMIT_OFFSETS;
    }

    public void onPartitionsRevoked(Collection<TopicPartition> partitionsRevoked) {
        String partitionsRevokedMsg = "Consumer group re-balance is about to begin. The following partitions will be revoked from this consumer: ";
        printPartitionsLogMsg(partitionsRevoked, partitionsRevokedMsg);

        String partitionsToCommitToMsg = "Committing currently marked offsets to the following partitions before consumer group re-balance: ";
        // don't need to synchronize on CURRENT_OFFSETS as ConcurrentHashMap is implemented to be thread-safe for reads and writes
        printPartitionsLogMsg(STAGED_OFFSETS.keySet(), partitionsToCommitToMsg);

        try {
            if (!STAGED_OFFSETS.isEmpty()) {
                KAFKA_LISTENER.commitSync(STAGED_OFFSETS);
                STAGED_OFFSETS.clear();
            }
        } catch (Throwable t) {
            log.error("Failed to commit offsets before consumer group re-balance.");
        }
    }

    public void onPartitionsAssigned(Collection<TopicPartition> partitionsAssigned) {
        String partitionsAssignedMsg = "Consumer group re-balance successful. The following partitions will be assigned to this consumer: ";
        printPartitionsLogMsg(partitionsAssigned, partitionsAssignedMsg);
    }

    private void printPartitionsLogMsg(Collection<TopicPartition> partitions, String baseLogMsg) {
        StringJoiner joiner = new StringJoiner(", ");
        StringBuilder partitionsLogMsg = new StringBuilder();
        partitionsLogMsg.append(baseLogMsg);

        for (TopicPartition partition : partitions) {
            joiner.add(String.valueOf(partition.partition()));
        }
        partitionsLogMsg.append(joiner.toString());
        log.info(partitionsLogMsg.toString());
    }
}
