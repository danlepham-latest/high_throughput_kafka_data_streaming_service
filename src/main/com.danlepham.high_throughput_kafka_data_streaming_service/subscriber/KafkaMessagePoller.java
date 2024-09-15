package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor;
import io.smallrye.common.constraint.Nullable;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface KafkaMessagePoller<K, V> {
    Uni<ScheduledExecutorService> startCommitScheduler(
            final KafkaRebalanceListener rebalanceListener,
            final long commitIntervalMs
    );
    Uni<Void> poll(final KafkaListenerContainer<K, V> listenerContainer);
    Uni<Void> applicationStart();
    Uni<Void> applicationShutdown(Throwable t, @Nullable String errorMsg);
    Uni<Void> commitSync(final KafkaConsumer<String, String> kafkaListener, final ConsumerRecord<String, String> recordToCommit);
    void commitAsync(final KafkaConsumer<K, V> kafkaListener, final KafkaRebalanceListener kafkaRebalanceListener);
    void consumeMessage(
            final ConsumerRecord<K, V> record,
            final KafkaRebalanceListener rebalanceListener,
            final MessageProcessor messageProcessor
    );
}
