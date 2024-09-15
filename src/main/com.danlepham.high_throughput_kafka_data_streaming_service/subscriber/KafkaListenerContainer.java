package com.danlepham.high_throughput_kafka_data_streaming_service.subscriber;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.processor.MessageProcessor;
import io.smallrye.mutiny.Uni;
import lombok.Getter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.Properties;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@Getter
class KafkaListenerContainer<K, V> {

    private static final Logger log = Logger.getLogger(KafkaListenerContainer.class);

    private final KafkaConsumer<K, V> kafkaListener;
    private final KafkaRebalanceListener rebalanceListener;
    private final String topicName;
    private final boolean pollingEnabled;
    private final long pollTimeoutMs;
    private final long pollDelayMs;
    private final long commitIntervalMs;
    private final boolean commitInBackground;
    private final MessageProcessor messageProcessor;
    private final int maxConcurrentMsgProcessors;
    //

    KafkaListenerContainer(SubscriberGroupConfig subscriberGroupConfig, MessageProcessor messageProcessor) {
        this.topicName = subscriberGroupConfig.getTopic();
        this.kafkaListener = createKafkaListener(subscriberGroupConfig.getKafkaListenerConfig());
        this.rebalanceListener = new KafkaRebalanceListener(kafkaListener);
        this.pollingEnabled = subscriberGroupConfig.isPollingEnabled();
        this.pollTimeoutMs = subscriberGroupConfig.getPollTimeoutMs();
        this.commitIntervalMs = subscriberGroupConfig.getCommitIntervalMs();
        this.commitInBackground = subscriberGroupConfig.isCommitInBackground();
        this.pollDelayMs = subscriberGroupConfig.getPollDelayMs();
        this.messageProcessor = messageProcessor;
        this.maxConcurrentMsgProcessors = subscriberGroupConfig.getMaxConcurrentMsgProcessorsPerSubscriber();
    }

    private KafkaConsumer<K, V> createKafkaListener(Properties kafkaListenerProps) {
        return new KafkaConsumer<>(kafkaListenerProps);
    }

    Uni<Boolean> start() {
        return Uni.createFrom().item(() -> {
            kafkaListener.subscribe(Collections.singletonList(topicName), rebalanceListener);
            return true;
        });
    }

    Uni<Boolean> stop() {
        return Uni.createFrom().item(() -> {
            synchronized (kafkaListener) {
                kafkaListener.close();
            }
            return true;
        });
    }
}
