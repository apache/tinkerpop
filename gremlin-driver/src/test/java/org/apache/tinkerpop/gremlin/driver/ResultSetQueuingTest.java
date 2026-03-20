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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for the internal queuing functionality of {@link ResultSet}.
 * 
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSetQueuingTest extends AbstractResultSetTest {

    @Test
    public void shouldGetSizeUntilError() throws Exception {
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
            assertThat(resultSet.getAvailableItemCount(), is(greaterThan(0)));
            assertThat(resultSet.allItemsAvailable(), is(false));

            final Exception theProblem = new Exception();
            resultSet.markError(theProblem);
            assertThat(resultSet.allItemsAvailable(), is(true));

            try {
                resultSet.getAvailableItemCount();
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
        assertThat(resultSet.isEmpty(), is(true));
        resultSet.add(new Result("test"));
        assertThat(resultSet.isEmpty(), is(false));
    }

    @Test
    public void shouldNotBeEmptyUntilError() throws Exception {
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
            assertThat(resultSet.isEmpty(), is(false));
            assertThat(resultSet.allItemsAvailable(), is(false));

            final Exception theProblem = new Exception();
            resultSet.markError(theProblem);
            assertThat(resultSet.allItemsAvailable(), is(true));

            try {
                resultSet.isEmpty();
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
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
            assertThat(resultSet.isEmpty(), is(false));
            final List<Result> drain = new ArrayList<>();
            resultSet.drainTo(drain);
            assertThat(drain.size(), is(greaterThan(0)));
            assertThat(resultSet.allItemsAvailable(), is(false));

            // make sure some more items get added to the result set before assert
            TimeUnit.MILLISECONDS.sleep(100);

            assertThat(resultSet.isEmpty(), is(false));
            assertThat(resultSet.allItemsAvailable(), is(false));

            final Exception theProblem = new Exception();
            resultSet.markError(theProblem);
            assertThat(resultSet.allItemsAvailable(), is(true));

            try {
                resultSet.drainTo(new ArrayList<>());
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
        final CompletableFuture<List<Result>> future = resultSet.some(4);
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));
        resultSet.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultSet.markComplete();
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultSet.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitFailTheFutureOnMarkError() {
        final CompletableFuture<List<Result>> future = resultSet.some(4);
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));
        resultSet.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultSet.markError(new Exception("no worky"));
        assertThat(future.isDone(), is(true));

        try {
            future.get();
        } catch (Exception ex) {
            final Throwable t = ExceptionUtils.getRootCause(ex);
            assertEquals("no worky", t.getMessage());
        }
    }

    @Test
    public void shouldAwaitToExpectedValueAndDrainOnAdd() throws Exception {
        final CompletableFuture<List<Result>> future = resultSet.some(3);
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));

        // shouldn't complete until the third item is in play
        assertThat(future.isDone(), is(false));

        resultSet.add(new Result("test3"));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultSet.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitMultipleToExpectedValueAndDrainOnAdd() throws Exception {
        final CompletableFuture<List<Result>> future1 = resultSet.some(3);
        final CompletableFuture<List<Result>> future2 = resultSet.some(1);
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));

        // shouldn't complete the first future until the third item is in play
        assertThat(future1.isDone(), is(false));
        assertThat(future2.isDone(), is(false));

        resultSet.add(new Result("test3"));

        final List<Result> results1 = future1.get();
        assertEquals("test1", results1.get(0).getString());
        assertEquals("test2", results1.get(1).getString());
        assertEquals("test3", results1.get(2).getString());
        assertEquals(3, results1.size());
        assertThat(future1.isDone(), is(true));
        assertThat(future2.isDone(), is(false));

        resultSet.add(new Result("test4"));
        assertThat(future1.isDone(), is(true));
        assertThat(future2.isDone(), is(true));

        final List<Result> results2 = future2.get();
        assertEquals("test4", results2.get(0).getString());
        assertEquals(1, results2.size());

        assertThat(resultSet.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitToExpectedValueAndDrainOnAwait() throws Exception {
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));
        resultSet.add(new Result("test3"));

        final CompletableFuture<List<Result>> future = resultSet.some(3);
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultSet.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitToReadCompletedAndDrainOnAwait() throws Exception {
        resultSet.add(new Result("test1"));
        resultSet.add(new Result("test2"));
        resultSet.add(new Result("test3"));

        resultSet.markComplete();

        // you might want 30 but there are only three
        final CompletableFuture<List<Result>> future = resultSet.some(30);
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultSet.isEmpty(), is(true));
    }

    @Test
    public void shouldDrainAsItemsArrive() throws Exception {
        final Thread t = addToQueue(1000, 1, true);
        try {
            final AtomicInteger count1 = new AtomicInteger(0);
            final AtomicInteger count2 = new AtomicInteger(0);
            final AtomicInteger count3 = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(3);

            resultSet.some(500).thenAcceptAsync(r -> {
                count1.set(r.size());
                latch.countDown();
            });

            resultSet.some(150).thenAcceptAsync(r -> {
                count2.set(r.size());
                latch.countDown();
            });

            resultSet.some(350).thenAcceptAsync(r -> {
                count3.set(r.size());
                latch.countDown();
            });

            assertThat(latch.await(10000, TimeUnit.MILLISECONDS), is(true));

            assertEquals(500, count1.get());
            assertEquals(150, count2.get());
            assertEquals(350, count3.get());

            assertThat(resultSet.isEmpty(), is(true));
        } finally {
            t.interrupt();
        }
    }
}
