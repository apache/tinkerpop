/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.fluency.glue;

import io.cucumber.guice.ScenarioScoped;
import io.cucumber.java.After;
import io.cucumber.java.Before;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Cucumber hooks that bound how long a scenario may run. Before each scenario a watchdog is armed that interrupts the
 * scenario's thread once the timeout passes, which makes the traversal throw a
 * {@code TraversalInterruptedException}. Cucumber has no timeout of its own.
 */
@ScenarioScoped
public class TraversalTimeout {
    private static final ScheduledExecutorService watchdog = Executors.newSingleThreadScheduledExecutor(r -> {
        final Thread t = new Thread(r, "gremlin-fluency-watchdog");
        t.setDaemon(true);
        return t;
    });

    private static volatile Duration timeout = Duration.ofSeconds(30);

    private ScheduledFuture<?> interrupt;

    /**
     * Sets the timeout for scenarios that start after this call.
     */
    public static void setTimeout(final Duration timeout) {
        if (timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("timeout must be positive");
        TraversalTimeout.timeout = timeout;
    }

    @Before
    public void arm() {
        final Thread scenarioThread = Thread.currentThread();
        interrupt = watchdog.schedule(scenarioThread::interrupt, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @After
    public void disarm() {
        if (interrupt != null) interrupt.cancel(false);

        // an interrupt that fired after the traversal finished must not leak into the next scenario
        Thread.interrupted();
    }
}
