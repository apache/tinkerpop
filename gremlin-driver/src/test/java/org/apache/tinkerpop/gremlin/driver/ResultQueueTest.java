/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultQueueTest {
    private LinkedBlockingQueue<Result> resultLinkedBlockingQueue;
    private CompletableFuture<Void> readCompleted;
    private ResultQueue resultQueue;

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @Before
    public void setup() {
        resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
        readCompleted = new CompletableFuture<>();
        resultQueue = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
    }

    @Test
    public void shouldGetSizeUntilError() throws Exception {
        final Thread t = addToQueue(100, 10);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.size(), is(greaterThan(0)));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);

            try {
                resultQueue.size();
                fail("Should have thrown an exception");
            } catch (Exception ex) {
                assertEquals(theProblem, ex.getCause());
            }
        } finally {
            t.interrupt();
        }
    }

    @Test
    public void shouldBeEmpty() {
        assertThat(resultQueue.isEmpty(), is(true));
    }

    @Test
    public void shouldNotBeEmptyUntilError() throws Exception {
        final Thread t = addToQueue(100, 10);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.isEmpty(), is(false));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);

            try {
                resultQueue.isEmpty();
                fail("Should have thrown an exception");
            } catch (Exception ex) {
                assertEquals(theProblem, ex.getCause());
            }
        } finally {
            t.interrupt();
        }
    }

    @Test
    public void shouldDrainUntilError() throws Exception {
        final Thread t = addToQueue(100, 10);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.isEmpty(), is(false));
            final List<Result> drain = new ArrayList<>();
            resultQueue.drainTo(drain);
            assertThat(drain.size(), is(greaterThan(0)));

            // make sure some more items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(100);

            assertThat(resultQueue.isEmpty(), is(false));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);

            try {
                resultQueue.drainTo(new ArrayList<>());
                fail("Should have thrown an exception");
            } catch (Exception ex) {
                assertEquals(theProblem, ex.getCause());
            }
        } finally {
            t.interrupt();
        }
    }

    private Thread addToQueue(final int numberOfItemsToAdd, final long pauseBetweenItemsInMillis) throws Exception {
        final Thread t = new Thread(() -> {
            boolean done = false;
            for (int ix = 0; ix < numberOfItemsToAdd && !done; ix++) {
                try {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    final int currentIndex = ix;
                    pool.submit(() -> {
                        final Result result = new Result("test-" + currentIndex);
                        resultQueue.add(result);
                    });
                    TimeUnit.MILLISECONDS.sleep(pauseBetweenItemsInMillis);
                } catch (InterruptedException ie) {
                    done = true;
                }
            }
        }, "ResultQueueTest-job-submitter");

        t.start();

        return t;
    }
}
