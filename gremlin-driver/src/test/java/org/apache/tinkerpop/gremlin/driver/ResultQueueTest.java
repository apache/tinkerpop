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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultQueueTest extends AbstractResultQueueTest {

    @Test
    public void shouldGetSizeUntilError() throws Exception {
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
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
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
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
        final Thread t = addToQueue(100, 10, true, false, 1);
        try {
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
    public void shouldAwaitFailTheFutureOnMarkError() throws Exception {
        final CompletableFuture<List<Result>> future = resultQueue.await(4);
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultQueue.markError(new Exception("no worky"));
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

    @Test
    public void shouldAwaitToExpectedValueAndDrainOnAwait() throws Exception {
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        final CompletableFuture<List<Result>> future = resultQueue.await(3);
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultQueue.isEmpty(), is(true));
    }

    @Test
    public void shouldAwaitToReadCompletedAndDrainOnAwait() throws Exception {
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        resultQueue.markComplete();

        // you might want 30 but there are only three
        final CompletableFuture<List<Result>> future = resultQueue.await(30);
        assertThat(future.isDone(), is(true));

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(resultQueue.isEmpty(), is(true));
    }

    @Test
    public void shouldDrainAsItemsArrive() throws Exception {
        final Thread t = addToQueue(1000, 1, true);
        try {
            final AtomicInteger count1 = new AtomicInteger(0);
            final AtomicInteger count2 = new AtomicInteger(0);
            final AtomicInteger count3 = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(3);

            resultQueue.await(500).thenAcceptAsync(r -> {
                count1.set(r.size());
                latch.countDown();
            });

            resultQueue.await(150).thenAcceptAsync(r -> {
                count2.set(r.size());
                latch.countDown();
            });

            resultQueue.await(350).thenAcceptAsync(r -> {
                count3.set(r.size());
                latch.countDown();
            });

            assertThat(latch.await(3000, TimeUnit.MILLISECONDS), is(true));

            assertEquals(500, count1.get());
            assertEquals(150, count2.get());
            assertEquals(350, count3.get());

            assertThat(resultQueue.isEmpty(), is(true));
        } finally {
            t.interrupt();
        }
    }

    @Test
    public void shouldHandleBulkSetSideEffects() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("stephen", 1));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        assertEquals(1, ((BulkSet) resultQueue.getSideEffect("a")).get("stephen"));

        resultQueue.addSideEffect("b", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("brian", 2));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("b"));
        assertEquals(2, ((BulkSet) resultQueue.getSideEffect("b")).get("brian"));

        resultQueue.addSideEffect("b", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("brian", 2));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("b"));
        assertEquals(4, ((BulkSet) resultQueue.getSideEffect("b")).get("brian"));

        resultQueue.addSideEffect("b", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("belinda", 6));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("b"));
        assertEquals(6, ((BulkSet) resultQueue.getSideEffect("b")).get("belinda"));

    }

    @Test
    public void shouldNotMixAggregatesForBulkSet() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("stephen", 1));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        assertEquals(1, ((BulkSet) resultQueue.getSideEffect("a")).get("stephen"));

        try {
            resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_BULKSET, Arrays.asList("stephen", "kathy", "alice"));
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
            assertEquals("Side-effect \"a\" value [stephen, kathy, alice] is a ArrayList which does not aggregate to bulkset", ex.getMessage());
        }
    }

    @Test
    public void shouldHandleListSideEffects() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_LIST, "stephen");
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        List<String> l = resultQueue.getSideEffect("a");
        assertEquals(1, l.size());
        assertEquals("stephen", l.get(0));

        resultQueue.addSideEffect("d", Tokens.VAL_AGGREGATE_TO_LIST, "daniel");
        assertThat(resultQueue.getSideEffectKeys(), hasItem("d"));
        l = resultQueue.getSideEffect("d");
        assertEquals(1, l.size());
        assertEquals("daniel", l.get(0));

        resultQueue.addSideEffect("d", Tokens.VAL_AGGREGATE_TO_LIST, "dave");
        assertThat(resultQueue.getSideEffectKeys(), hasItem("d"));
        l = resultQueue.getSideEffect("d");
        assertEquals(2, l.size());
        assertThat(l, contains("daniel","dave"));
    }

    @Test
    public void shouldNotMixAggregatesForList() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_BULKSET, new RemoteTraverser<>("stephen", 1));
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        assertEquals(1, ((BulkSet) resultQueue.getSideEffect("a")).get("stephen"));

        try {
            resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_LIST, Arrays.asList("stephen", "kathy", "alice"));
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
            assertEquals("Side-effect \"a\" contains the type BulkSet that is not acceptable for list", ex.getMessage());
        }
    }

    @Test
    public void shouldHandleMapSideEffects() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        final Map<String,String> m = new HashMap<>();
        m.put("s", "stephen");
        m.put("m", "marko");
        m.put("d", "daniel");

        m.entrySet().forEach(e -> {
            resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_MAP, e);
            assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
            assertEquals(e.getValue(), ((Map) resultQueue.getSideEffect("a")).get(e.getKey()));
        });

        assertEquals(3, ((Map) resultQueue.getSideEffect("a")).size());
    }

    @Test
    public void shouldNotMixAggregatesForMap() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        final Map<String,String> m = new HashMap<>();
        m.put("s", "stephen");

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_MAP, m.entrySet().iterator().next());
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        assertEquals("stephen", ((Map) resultQueue.getSideEffect("a")).get("s"));

        try {
            resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_MAP, Arrays.asList("stephen", "kathy", "alice"));
        } catch (Exception ex) {
            assertThat(ex, instanceOf(IllegalStateException.class));
            assertEquals("Side-effect \"a\" value [stephen, kathy, alice] is a ArrayList which does not aggregate to map", ex.getMessage());
        }
    }

    @Test
    public void shouldHandleNotAggregateSideEffects() {
        assertThat(resultQueue.getSideEffectKeys().isEmpty(), is(true));

        final Map<String,String> m = new HashMap<>();
        m.put("s", "stephen");
        m.put("m", "marko");
        m.put("d", "daniel");

        resultQueue.addSideEffect("a", Tokens.VAL_AGGREGATE_TO_NONE, m);
        assertThat(resultQueue.getSideEffectKeys(), hasItem("a"));
        assertEquals("stephen", ((Map) resultQueue.getSideEffect("a")).get("s"));
        assertEquals("marko", ((Map) resultQueue.getSideEffect("a")).get("m"));
        assertEquals("daniel", ((Map) resultQueue.getSideEffect("a")).get("d"));
        assertEquals(3, ((Map) resultQueue.getSideEffect("a")).size());
    }
}
