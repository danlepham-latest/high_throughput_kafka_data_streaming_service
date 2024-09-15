package com.danlepham.high_throughput_kafka_data_streaming_service.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-06-2021
 */

public class Util {

    public static ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    public static final int KAFKA_MAX_RETRIES = 4;
    public static final long KAFKA_BACKOFF_DELAY = 10000L;
    public static final long KAFKA_TIMEOUT_IN_SECONDS = 120L;
    //

    /* HASHING FUNCTIONS */
    private static HashFunction hashFunction = Hashing.murmur3_128();

    public static Long hash(String content) {
        return hash(content, StandardCharsets.UTF_8);
    }

    public static Long hash(String keyToHash, Charset charset) {
        Charset charset_ = charset != null ? charset : StandardCharsets.UTF_8;
        return hashFunction.newHasher()
                .putBytes(keyToHash.getBytes(charset_))
                .hash()
                .asLong();
    }

    /* DATE TIME CONVERSION FUNCTIONS */
    public static DateTimeFormatter simpleDateFormatterV1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static DateTimeFormatter simpleDateFormatterV2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    public static DateTimeFormatter basicDateFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    public static LocalDateTime getCurrentTimestamp() {
        LocalDateTime insertTimestamp = new Timestamp(System.currentTimeMillis()).toLocalDateTime();
        String insertTimestampStr = insertTimestamp.format(simpleDateFormatterV2);
        return LocalDateTime.parse(insertTimestampStr, simpleDateFormatterV2);
    }

    public static LocalDateTime parseBasicDateTimeToLocalDateTime(String basicDateTimeStr) {
        // parse basic date time str into localdatetime obj
        LocalDateTime localDateTime = LocalDateTime.parse(basicDateTimeStr, basicDateFormatter);

        // convert basic date obj to a simple date time str, and then to a localdatetime obj
        String simpleDateTimeStr = localDateTime.format(simpleDateFormatterV1);
        localDateTime = LocalDateTime.parse(simpleDateTimeStr, simpleDateFormatterV1);

        return localDateTime;
    }

    /* EXCEPTION HANDLING FUNCTIONS */
    public static String parseExceptionToString(Throwable t) {
        StringWriter stackTrace = new StringWriter();
        t.printStackTrace(new PrintWriter(stackTrace));
        return stackTrace.toString();
    }
}
