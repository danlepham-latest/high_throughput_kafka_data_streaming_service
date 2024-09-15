package com.danlepham.high_throughput_kafka_data_streaming_service.util;

import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Dan Lepham (dan.lepham@gmail.com)
 * @since 10-18-2021
 */

public final class WorkerThreadFactory implements ThreadFactory {

    private static final String DEFAULT_THREAD_NAME_PREFIX = "DATA-STREAMING-WORKER";
    private static final AtomicInteger THREAD_POOL_NUM = new AtomicInteger(1);
    private final ThreadGroup THREAD_GROUP;
    private final AtomicInteger THREAD_NUM = new AtomicInteger(1);
    private final String THREAD_POOL_NAME_PREFIX;

    public WorkerThreadFactory() {
        this(DEFAULT_THREAD_NAME_PREFIX);
    }

    public WorkerThreadFactory(String threadNamePrefix) {

        Optional<SecurityManager> securityManagerOpt = Optional.ofNullable(System.getSecurityManager());
        if (securityManagerOpt.isPresent()) {
            SecurityManager securityManager = securityManagerOpt.get();
            THREAD_GROUP = securityManager.getThreadGroup();
        } else {
            THREAD_GROUP = Thread.currentThread().getThreadGroup();
        }

        // e.g. "WORKER-POOL-1_THREAD-"
        THREAD_POOL_NAME_PREFIX = threadNamePrefix + "-POOL-" + THREAD_POOL_NUM.getAndIncrement() + "_THREAD-";
    }

    @Override
    public Thread newThread(Runnable runnableTask) {
        // e.g. "WORKER-POOL-1_THREAD-23"
        String threadName = THREAD_POOL_NAME_PREFIX + THREAD_NUM.getAndIncrement();
        Thread newCustomThread = new Thread(THREAD_GROUP, runnableTask, threadName, 0);

        if (newCustomThread.isDaemon()) {
            newCustomThread.setDaemon(false);
        }

        if (newCustomThread.getPriority() != Thread.NORM_PRIORITY) {
            newCustomThread.setPriority(Thread.NORM_PRIORITY);
        }

        return newCustomThread;
    }
}
