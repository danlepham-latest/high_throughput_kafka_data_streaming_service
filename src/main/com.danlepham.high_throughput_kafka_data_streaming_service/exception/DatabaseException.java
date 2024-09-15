package com.danlepham.high_throughput_kafka_data_streaming_service.exception;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public class DatabaseException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public DatabaseException(final String errorMessage) {
        super(errorMessage);
    }

    public DatabaseException(final String errorMessage, Throwable cause) {
        super(errorMessage, cause);
    }
}
