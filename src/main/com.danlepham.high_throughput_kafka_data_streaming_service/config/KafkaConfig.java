package com.danlepham.high_throughput_kafka_data_streaming_service.config;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupConfig;

import java.util.List;
import java.util.Properties;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface KafkaConfig {

    String getApplicationName();
    boolean isDLQEnabled();
    boolean isAsyncPublishingEnabled();
    boolean isCanonicalPublicationEnabled();
    List<SubscriberGroupConfig> buildSubscriberGroupConfigs();
    void appendKerberosProps(Properties clientProps);
    void initKerberosEnvironmentVars();
}
