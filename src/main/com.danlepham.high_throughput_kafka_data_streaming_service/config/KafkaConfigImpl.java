package com.danlepham.high_throughput_kafka_data_streaming_service.config;

import com.danlepham.high_throughput_kafka_data_streaming_service.dto.KerberosProperties;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.PublisherProperties;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupConfig;
import com.danlepham.high_throughput_kafka_data_streaming_service.dto.SubscriberGroupProperties;
import io.quarkus.runtime.Startup;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

@Setter
@Startup
@NoArgsConstructor
@ApplicationScoped
@KafkaConfigQualifier
public class KafkaConfigImpl implements KafkaConfig {

    private static final Logger log = Logger.getLogger(KafkaConfigImpl.class);

    private KafkaProperties kafkaProperties;

    private String applicationName;
    private KerberosProperties kerberosProps;
    private PublisherProperties publisherProps;
    Map<String, SubscriberGroupProperties> subscriberGroups;

    @Inject
    public KafkaConfigImpl(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }
    //

    @PostConstruct
    public void initProperties() {
        this.applicationName = kafkaProperties.applicationName();
        this.kerberosProps = kafkaProperties.kerberosProps();
        this.publisherProps = kafkaProperties.publisherProps();
        this.subscriberGroups = kafkaProperties.subscriberGroups();
    }

    public String getApplicationName() {
        return applicationName;
    }

    public boolean isDLQEnabled() {
        return publisherProps.dlqEnabled();
    }

    public boolean isAsyncPublishingEnabled() { return publisherProps.asyncPublishingEnabled(); }

    public boolean isCanonicalPublicationEnabled() {
        return publisherProps.canonicalPublicationEnabled();
    }

    // SUBSCRIBER CONFIGURATION
    public List<SubscriberGroupConfig> buildSubscriberGroupConfigs() {
        List<SubscriberGroupConfig> subscriberGroupConfigs = new ArrayList<>();
        subscriberGroups.forEach((groupKey, subscriberGroup) -> {
            log.info("Building Kafka consumer group configuration for " + groupKey);

            Properties consumerProps = new Properties();

            // The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect. If provided, the backoff per host will increase exponentially for each consecutive connection failure, up to this maximum. After calculating the backoff increase, 20% random jitter is added to avoid connection storms.
            // Default:	1000 ms (1 second) (type long)
            consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, String.valueOf(subscriberGroup.reconnectBackoffMaxMs()));

            // The base amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all connection attempts by the client to a broker.
            // Default:	50 (type long)
            consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, String.valueOf(subscriberGroup.reconnectBackoffMs()));

            consumerProps.setProperty(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(subscriberGroup.retryBackoffMs())); // Default: 100 (type long)
            consumerProps.setProperty(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, String.valueOf(subscriberGroup.connectionsMaxIdleMs())); // Close idle connections after the number of milliseconds specified by this config. DEFAULT: 540000 (9 minutes) (type long)

            // The configuration controls the maximum amount of time the client will wait for the response of a request. If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted.
            consumerProps.setProperty(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, String.valueOf(subscriberGroup.requestTimeoutMs())); // default 30000 ms or 30 seconds (Type int)

            // The timeout used to detect client failures when using Kafka's group management facility. The client sends periodic heartbeats to indicate its liveness to the broker. If no heartbeats are received by the broker before the expiration of this session timeout, then the broker will remove this client from the group and initiate a rebalance. Note that the value must be in the allowable range as configured in the broker configuration by group.min.session.timeout.ms and group.max.session.timeout.ms.
            consumerProps.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, String.valueOf(subscriberGroup.sessionTimeoutMs())); // Default: 10000 (10 seconds (type int)

            // The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities. Heartbeats are used to ensure that the consumer's session stays active and to facilitate rebalancing when new consumers join or leave the group. The value must be set lower than session.timeout.ms, but typically should be set no higher than 1/3 of that value. It can be adjusted even lower to control the expected time for normal rebalances.
            consumerProps.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, String.valueOf(subscriberGroup.heartbeatIntervalMs())); // Default: 3000 (3 seconds) (type int)

            // The maximum number of records returned in a single call to poll(). Note, that max.poll.records does not impact the underlying fetching behavior. The consumer will cache the records from each fetch request and returns them incrementally from each poll.
            consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(subscriberGroup.maxPollRecords())); // Default: 500 (type int)

            // The maximum delay between invocations of poll() when using consumer group management. This places an upper bound on the amount of time that the consumer can be idle before fetching more records. If poll() is not called before expiration of this timeout, then the consumer is considered failed and the group will rebalance in order to reassign the partitions to another member. For consumers using a non-null group.instance.id which reach this timeout, partitions will not be immediately reassigned. Instead, the consumer will stop sending heartbeats and partitions will be reassigned after expiration of session.timeout.ms. This mirrors the behavior of a static consumer which has shutdown.
            consumerProps.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, String.valueOf(subscriberGroup.maxPollIntervalMs())); // Default: 300000 (5 minutes) (type int)

            consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, subscriberGroup.bootstrapServers());
            consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, subscriberGroup.groupId());
            consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, subscriberGroup.autoOffsetReset());
            consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(subscriberGroup.enableAutoCommit()));

            consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, subscriberGroup.deserializer());
            consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, subscriberGroup.deserializer());

            appendKerberosProps(consumerProps);

            SubscriberGroupConfig subscriberGroupConfig = SubscriberGroupConfig.builder()
                    .topic(subscriberGroup.topic())
                    .numOfSubscribers(subscriberGroup.numberOfSubscribers())
                    .maxConcurrentMsgProcessorsPerSubscriber(subscriberGroup.maxConcurrentMsgProcessorsPerSubscriber())
                    .pollingEnabled(subscriberGroup.pollingEnabled())
                    .pollTimeoutMs(subscriberGroup.pollTimeoutMs())
                    .commitIntervalMs(subscriberGroup.commitIntervalMs())
                    .pollDelayMs(subscriberGroup.pollDelayMs())
                    .messageProcessor(subscriberGroup.messageProcessor())
                    .commitInBackground(subscriberGroup.commitInBackground())
                    .kafkaListenerConfig(consumerProps)
                    .build();

            subscriberGroupConfigs.add(subscriberGroupConfig);
        });

        return subscriberGroupConfigs;
    }

    // KERBEROS CONFIGURATION
    public void appendKerberosProps(Properties clientProps) {
        clientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, kerberosProps.sslTruststoreLocation());
        clientProps.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, kerberosProps.sslTruststorePassword());
        clientProps.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kerberosProps.securityProtocol());
        clientProps.setProperty(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
    }

    public void initKerberosEnvironmentVars() {
        System.setProperty("java.security.auth.login.config", kerberosProps.javaSecurityAuthLoginConfig());
        System.setProperty("java.security.krb5.realm", kerberosProps.javaSecurityKrb5Realm());
        System.setProperty("java.security.krb5.kdc", kerberosProps.javaSecurityKrb5Kdc());
    }
}
