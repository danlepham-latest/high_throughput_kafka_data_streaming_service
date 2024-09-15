package com.danlepham.high_throughput_kafka_data_streaming_service.dto;

import io.smallrye.config.WithDefault;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface PublisherProperties {
    @WithDefault("false")
    boolean dlqEnabled();
    @WithDefault("true")
    boolean asyncPublishingEnabled();
    @WithDefault("false")
    boolean canonicalPublicationEnabled();
}
