package com.danlepham.high_throughput_kafka_data_streaming_service.processor;

import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-18-2021
 */

@NoArgsConstructor
@ApplicationScoped
public class TestMessageProcessorImpl implements MessageProcessor {

    private static final Logger log = Logger.getLogger(TestMessageProcessorImpl.class);

    public void processMessage(ConsumerRecord<String, String> message) {
        String logMsg = String.format("On topic %1$s, offset %2$s from partition %3$s has been processed.",
                message.topic(), message.offset(), message.partition());
        log.debug(logMsg);
    }
}
