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
    private CompletableFuture<Void> readCompleted;
    private ResultQueue resultQueue;

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @Before
    public void setup() {
        LinkedBlockingQueue<Result> resultLinkedBlockingQueue = new LinkedBlockingQueue<>();
        readCompleted = new CompletableFuture<>();
        resultQueue = new ResultQueue(resultLinkedBlockingQueue, readCompleted);
    }

    @Test
    public void shouldGetSizeUntilError() throws Exception {
        final Thread t = addToQueue(100, 10, true);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.size(), is(greaterThan(0)));
            assertThat(readCompleted.isDone(), is(false));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);
            assertThat(readCompleted.isDone(), is(true));

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
    public void shouldBeEmptyThenNotEmpty() {
        assertThat(resultQueue.isEmpty(), is(true));
        resultQueue.add(new Result("test"));
        assertThat(resultQueue.isEmpty(), is(false));
    }

    @Test
    public void shouldNotBeEmptyUntilError() throws Exception {
        final Thread t = addToQueue(100, 10, true);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.isEmpty(), is(false));
            assertThat(readCompleted.isDone(), is(false));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);
            assertThat(readCompleted.isDone(), is(true));

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
        final Thread t = addToQueue(100, 10, true);
        try {
            // make sure some items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(50);

            assertThat(resultQueue.isEmpty(), is(false));
            final List<Result> drain = new ArrayList<>();
            resultQueue.drainTo(drain);
            assertThat(drain.size(), is(greaterThan(0)));
            assertThat(readCompleted.isDone(), is(false));

            // make sure some more items get added to the queue before assert
            TimeUnit.MILLISECONDS.sleep(100);

            assertThat(resultQueue.isEmpty(), is(false));
            assertThat(readCompleted.isDone(), is(false));

            final Exception theProblem = new Exception();
            resultQueue.markError(theProblem);
            assertThat(readCompleted.isDone(), is(true));

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

    @Test
    public void shouldAwaitEverythingAndFlushOnMarkCompleted() throws Exception {
        final CompletableFuture<List<Result>> future = resultQueue.await(4);
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultQueue.markComplete();
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultQueue.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitEverythingAndFlushOnMarkError() throws Exception {
        // not so sure this is good behavior (i.e. flushing whatever has arrived up to the error)
        final CompletableFuture<List<Result>> future = resultQueue.await(4);
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultQueue.markError(new Exception());
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());
    }

    @Test
    public void shouldAwaitToExpectedValueAndDrainOnAdd() throws Exception {
        final CompletableFuture<List<Result>> future = resultQueue.await(3);
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));

        // shouldn't complete until the third item is in play
        assertThat(future.isDone(), is(false));

        resultQueue.add(new Result("test3"));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultQueue.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitMultipleToExpectedValueAndDrainOnAdd() throws Exception {
        final CompletableFuture<List<Result>> future1 = resultQueue.await(3);
        final CompletableFuture<List<Result>> future2 = resultQueue.await(1);
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));

        // shouldn't complete the first future until the third item is in play
        assertThat(future1.isDone(), is(false));
        assertThat(future2.isDone(), is(false));

        resultQueue.add(new Result("test3"));

        final List<Result> results1 = future1.get();
        assertEquals("test1", results1.get(0).getString());
        assertEquals("test2", results1.get(1).getString());
        assertEquals("test3", results1.get(2).getString());
        assertEquals(3, results1.size());
        assertThat(future1.isDone(), is(true));
        assertThat(future2.isDone(), is(false));

        resultQueue.add(new Result("test4"));
        assertThat(future1.isDone(), is(true));
        assertThat(future2.isDone(), is(true));

        final List<Result> results2 = future2.get();
        assertEquals("test4", results2.get(0).getString());
        assertEquals(1, results2.size());

        assertThat(resultQueue.isEmpty(), is(true));
    }

    private Thread addToQueue(final int numberOfItemsToAdd, final long pauseBetweenItemsInMillis,
                              final boolean start) throws Exception {
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

        if (start) t.start();

        return t;
    }
}
