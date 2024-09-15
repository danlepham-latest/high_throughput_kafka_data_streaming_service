package com.danlepham.high_throughput_kafka_data_streaming_service.publisher;

import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.config.KafkaConfigQualifier;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.AbstractDto;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.DeadLetterMetadata;
import com.danlepham.high_throughput_kafka_data_streaming_service.util.Util;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.smallrye.mutiny.Uni;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@NoArgsConstructor
@ApplicationScoped
@KafkaMessagePublisherQualifier
public class KafkaMessagePublisherImpl implements KafkaMessagePublisher<AbstractDto, String> {

    private static final Logger log = Logger.getLogger(KafkaMessagePublisherImpl.class);

    private KafkaConfig kafkaConfig;
    private Emitter<String> dlqEmitter;

    @Inject
    public KafkaMessagePublisherImpl(
            @KafkaConfigQualifier KafkaConfig kafkaConfig,
            @Channel("dead-letter-queue") Emitter<String> dlqEmitter) {

        this.kafkaConfig = kafkaConfig;
        this.dlqEmitter = dlqEmitter;
    }
    //

    public CompletableFuture<Void> publishToDeadLetterQueue(String json) {
        if (kafkaConfig.isDLQEnabled()) {
            return dlqEmitter.send(json).toCompletableFuture();
        }
        return CompletableFuture.completedFuture(null);
    }

    public Uni<Void> publishToDeadLetterQueue(final ConsumerRecord<String, String> record, String srcTopic, Throwable error) {
        if (kafkaConfig.isDLQEnabled()) {
            return Uni.createFrom().completionStage(CompletableFuture.runAsync(() -> {
                try {
                    String applicationName = kafkaConfig.getApplicationName();
                    DeadLetterMetadata deadLetterMetadata = DeadLetterMetadata.builder()
                            .sourceName(applicationName)
                            .sourceTopic(srcTopic)
                            .timeErrorOccurred(Util.getCurrentTimestamp())
                            .errorReason(error.getMessage())
                            .build();

                    String dlqMetadataJsonStr = Util.mapper.writeValueAsString(deadLetterMetadata);
                    JsonObject dlqMetadataJsonObj = JsonParser.parseString(dlqMetadataJsonStr).getAsJsonObject();
                    JsonElement badMessageJsonElement = JsonParser.parseString(record.value());
                    dlqMetadataJsonObj.add("BAD_MESSAGE", badMessageJsonElement);

                    if (!dlqMetadataJsonObj.isJsonNull()) {
                        String jsonToPublish = dlqMetadataJsonObj.toString();

                        if (kafkaConfig.isAsyncPublishingEnabled()) {
                            publishToDeadLetterQueue(jsonToPublish);
                        } else {
                            publishToDeadLetterQueue(jsonToPublish).get();
                        }
                    }
                } catch (Throwable ignored) {
                    // if unable to publish to dead letter topic (most likely smtp/kafka server reasons), then just move on
                    // don't emit the exception or anything to the caller and just move on
                    log.error("Failed to publish to dead letter topic. Reason: ", ignored);
                }
            }));
        }

        return Uni.createFrom().nullItem();
    }
}
