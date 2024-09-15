package com.danlepham.high_throughput_kafka_data_streaming_service.processor;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@FunctionalInterface
public interface MessageProcessor {
    void processMessage(ConsumerRecord<String, String> message) throws Exception;
}
