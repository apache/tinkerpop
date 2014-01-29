package com.tinkerpop.gremlin.server.util;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LocalExecutorServiceTest {
    @Test
    public void shouldBindToSameThread() throws Exception {
        final ExecutorService executorService1 = LocalExecutorService.getLocal();
        final ExecutorService executorService2 = LocalExecutorService.getLocal();

        assertEquals(executorService1, executorService2);
        assertSame(executorService1, executorService2);

        final AtomicBoolean same = new AtomicBoolean(false);
        final AtomicBoolean equals = new AtomicBoolean(false);
        final AtomicBoolean different = new AtomicBoolean(false);
        final Thread t = new Thread(() -> {
            final ExecutorService executorServiceDifferent1 = LocalExecutorService.getLocal();
            final ExecutorService executorServiceDifferent2 = LocalExecutorService.getLocal();

            equals.set(executorServiceDifferent1.equals(executorServiceDifferent2));
            same.set(executorServiceDifferent1 == executorServiceDifferent2);

            different.set(!executorService1.equals(executorServiceDifferent1));
        });

        t.start();
        t.join();

        assertTrue(equals.get());
        assertTrue(same.get());
        assertTrue(different.get());
    }
}
