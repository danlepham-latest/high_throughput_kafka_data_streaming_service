package com.danlepham.high_throughput_kafka_data_streaming_service.dto;

import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface SubscriberGroupProperties {
    String messageProcessor();
    String topic();
    String bootstrapServers();
    String groupId();

    @WithName("commit-in-background")
    boolean commitInBackground();
    @WithDefault("org.apache.kafka.common.serialization.StringDeserializer")
    String deserializer();
    @WithDefault("5000")
    long commitIntervalMs();
    @WithDefault("1")
    int maxConcurrentMsgProcessorsPerSubscriber();
    @WithDefault("false")
    boolean pollingEnabled();
    @WithDefault("10000")
    long pollTimeoutMs();
    @WithDefault("1000")
    long pollDelayMs();
    @WithDefault("1")
    int numberOfSubscribers();

    @WithDefault("earliest")
    String autoOffsetReset();
    @WithDefault("false")
    boolean enableAutoCommit();

    @WithDefault("5000")
    long reconnectBackoffMaxMs();
    @WithDefault("60000")
    long reconnectBackoffMs();
    @WithDefault("30000")
    int requestTimeoutMs();
    @WithDefault("30000")
    long retryBackoffMs();
    @WithDefault("540000")
    long connectionsMaxIdleMs();
    @WithDefault("600000")
    int sessionTimeoutMs();
    @WithDefault("1000")
    int heartbeatIntervalMs();
    @WithDefault("900000")
    int maxPollIntervalMs();
    @WithDefault("500")
    int maxPollRecords();
}
