package com.danlepham.high_throughput_kafka_data_streaming_service.dto;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public interface KerberosProperties {
    String securityProtocol();
    String javaSecurityAuthLoginConfig();
    String javaSecurityKrb5Realm();
    String javaSecurityKrb5Kdc();

    String sslTruststoreLocation();
    String sslTruststorePassword();
}
