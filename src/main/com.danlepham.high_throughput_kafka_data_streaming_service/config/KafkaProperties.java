package com.danlepham.high_throughput_kafka_data_streaming_service.config;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.KerberosProperties;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.PublisherProperties;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupProperties;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

import java.util.Map;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@ConfigMapping(prefix = "mq-custom.kafka-props", namingStrategy = ConfigMapping.NamingStrategy.KEBAB_CASE)
public interface KafkaProperties {
    @WithName("application-name")
    String applicationName();
    @WithName("kerberos-props")
    KerberosProperties kerberosProps();
    @WithName("publisher-props")
    PublisherProperties publisherProps();
    @WithName("subscriber-groups")
    Map<String, SubscriberGroupProperties> subscriberGroups();
}
