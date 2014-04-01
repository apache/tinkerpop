package com.tinkerpop.gremlin.server.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Provides a static way to get a {@link ThreadLocal} "single thread executor". In this way any thread can have its own
 * "worker" thread.  The {@link ExecutorService} is instantiated per thread via
 * {@link Executors#newSingleThreadExecutor()}
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LocalExecutorService {

    private static ThreadLocal<ExecutorService> threadLocalExecutorService = new ThreadLocal<ExecutorService>() {
        @Override
        protected ExecutorService initialValue() {
            return Executors.newSingleThreadExecutor(r -> new Thread(r, "gremlin-executor"));
        }
    };

    /**
     * Gets the {@link ExecutorService} for the current thread.
     */
    public static ExecutorService getLocal() {
        return threadLocalExecutorService.get();
    }
}
