package com.danlepham.high_throughput_kafka_data_streaming_service.dto;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@Getter
@Setter
@Builder
public class SubscriberGroupConfig {
    private String topic;
    private int numOfSubscribers;
    private int maxConcurrentMsgProcessorsPerSubscriber;
    private boolean pollingEnabled;
    private long pollTimeoutMs;
    private long pollDelayMs;
    private long commitIntervalMs;
    private String messageProcessor;
    private boolean commitInBackground;

    private Properties kafkaListenerConfig;
}
