package com.danlepham.high_throughput_kafka_data_streaming_service.publisher;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.AbstractDto;
import io.smallrye.mutiny.Uni;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface KafkaMessagePublisher<T extends AbstractDto, U> {
    CompletableFuture<Void> publishToDeadLetterQueue(U data);
    Uni<Void> publishToDeadLetterQueue(
            ConsumerRecord<String, String> record,
            String srcTopic,
            Throwable error
    );
}
