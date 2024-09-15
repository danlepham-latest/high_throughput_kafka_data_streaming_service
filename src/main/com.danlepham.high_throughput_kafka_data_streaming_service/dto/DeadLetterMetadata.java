package com.danlepham.high_throughput_kafka_data_streaming_service.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@Getter
@Setter
@Builder
public class DeadLetterMetadata {
    private String sourceName;
    private String sourceTopic;
    @JsonFormat(pattern="yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timeErrorOccurred;
    private String errorReason;
}