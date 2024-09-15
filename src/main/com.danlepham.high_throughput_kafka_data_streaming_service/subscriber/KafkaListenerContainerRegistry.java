package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor;
import io.smallrye.mutiny.Uni;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface KafkaListenerContainerRegistry<K, V> {
    Uni<Void> initKafkaClientConfigs();
    List<SubscriberGroupConfig> getSubscriberGroupConfigs();
    List<KafkaListenerContainer<K, V>> getKafkaListenerContainers();
    Uni<MessageProcessor> getMessageProcessor(final String msgProcessorProp);
    Uni<Void> initKafkaListenerContainerRegistry();

    Uni<KafkaListenerContainer<K, V>> createKafkaListenerContainer(
            SubscriberGroupConfig subscriberGroupConfig,
            MessageProcessor messageProcessor
    );

    Uni<Void> start();
    Uni<Void> stop();
    void awaitMsgListenerTasks(List<CompletableFuture<Boolean>> msgListenerTasks);
}
