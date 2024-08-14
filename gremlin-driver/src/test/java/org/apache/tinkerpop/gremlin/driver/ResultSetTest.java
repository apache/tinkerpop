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

import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultSetTest extends AbstractResultQueueTest {

    private ResultSet resultSet;

    @Before
    public void setupThis() {
        resultSet = new ResultSet(resultQueue, pool, readCompleted, RequestMessage.build("traversal").create(), null);
    }

    @Test
    public void shouldHaveAllItemsAvailableAsynchronouslyOnReadComplete() throws InterruptedException {
        final CompletableFuture<Void> all = resultSet.allItemsAvailableAsync();
        assertThat(all.isDone(), is(false));
        readCompleted.complete(null);
        // flush all tasks in pool
        pool.awaitTermination(2, TimeUnit.SECONDS);
        assertThat(all.isDone(), is(true));
    }

    @Test
    public void shouldHaveAllItemsAvailableAsynchronouslyOnReadCompleteExceptionally() throws InterruptedException {
        final CompletableFuture<Void> all = resultSet.allItemsAvailableAsync();
        assertThat(all.isDone(), is(false));
        readCompleted.completeExceptionally(new RuntimeException());
        // flush all tasks in pool
        pool.awaitTermination(2, TimeUnit.SECONDS);
        assertThat(all.isDone(), is(true));
        assertThat(all.isCompletedExceptionally(), is(true));
    }

    @Test
    public void shouldHaveAllItemsAvailableOnReadComplete() throws InterruptedException {
        assertThat(resultSet.allItemsAvailable(), is(false));
        readCompleted.complete(null);
        // flush all tasks in pool
        pool.awaitTermination(2, TimeUnit.SECONDS);
        assertThat(resultSet.allItemsAvailable(), is(true));
    }

    @Test
    public void shouldHaveAllItemsAvailableAsynchronouslyOnReadCompleteWhileLoading() throws Exception {
        final CompletableFuture<Void> all = resultSet.allItemsAvailableAsync();
        assertThat(all.isDone(), is(false));

        final AtomicBoolean atLeastOnce = new AtomicBoolean(false);
        addToQueue(1000, 1, true, true);
        while (!readCompleted.isDone()) {
            atLeastOnce.set(true);
            if (!atLeastOnce.get())
                assertThat(all.isDone(), is(false));
        }

        assertThat(atLeastOnce.get(), is(true));

        // ensure there is enough time for the readComplete to complete the "all" future
        all.get(30000, TimeUnit.MILLISECONDS);
        assertThat(all.isDone(), is(true));
    }

    @Test
    public void shouldHaveAllItemsAvailableOnReadCompleteWhileLoading() throws Exception {
        assertThat(resultSet.allItemsAvailable(), is(false));

        final AtomicBoolean atLeastOnce = new AtomicBoolean(false);
        addToQueue(1000, 1, true, true);
        while (!readCompleted.isDone()) {
            atLeastOnce.set(true);
            if (!atLeastOnce.get())
                assertThat(resultSet.allItemsAvailable(), is(false));
        }

        assertThat(atLeastOnce.get(), is(true));
        assertThat(resultSet.allItemsAvailable(), is(true));
    }

    @Test
    public void shouldAwaitEverythingAndFlushOnMarkCompleted() throws Exception {
        final CompletableFuture<List<Result>> future = resultSet.some(4);
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

        assertThat(resultSet.allItemsAvailable(), is(true));
        assertEquals(0, resultSet.getAvailableItemCount());
    }

    @Test
    public void shouldGetAllOnlyOnComplete() throws Exception {
        final CompletableFuture<List<Result>> future = resultSet.all();
        resultQueue.add(new Result("test1"));
        resultQueue.add(new Result("test2"));
        resultQueue.add(new Result("test3"));

        assertThat(future.isDone(), is(false));
        resultQueue.markComplete();

        final List<Result> results = future.get();
        assertEquals("test1", results.get(0).getString());
        assertEquals("test2", results.get(1).getString());
        assertEquals("test3", results.get(2).getString());
        assertEquals(3, results.size());

        assertThat(future.isDone(), is(true));
        assertThat(resultSet.allItemsAvailable(), is(true));
        assertEquals(0, resultSet.getAvailableItemCount());
    }

    @Test
    public void shouldIterate() throws Exception {
        final Iterator itty = resultSet.iterator();
        final AtomicInteger counter = new AtomicInteger(0);

        addToQueue(100, 1, true, true);

        while (itty.hasNext()) {
            itty.next();
            counter.incrementAndGet();
        }

        assertEquals(100, counter.get());
    }

    @Test
    public void shouldIterateWithoutCheckingHasNext() throws Exception {
        final Iterator itty = resultSet.iterator();
        final AtomicInteger counter = new AtomicInteger(0);

        addToQueue(100, 1, true, true);

        for (int ix = 0; ix < 100; ix++) {
            itty.next();
            counter.incrementAndGet();
        }

        assertEquals(100, counter.get());
        assertThat(itty.hasNext(), is(false));
    }

    @Test
    public void shouldIterateAndThenThrowIfCallToNextExceedsAvailableResultItems() throws Exception {
        final Iterator itty = resultSet.iterator();

        addToQueue(3, 1, true, true);

        assertThat(itty.hasNext(), is(true));
        itty.next();
        assertThat(itty.hasNext(), is(true));
        itty.next();
        itty.next();
        assertThat(itty.hasNext(), is(false));

        try {
            itty.next();
            fail("Should throw exception");
        } catch (NoSuchElementException ignore) {
            // this is the right path - an exception should toss here
        }
    }

    @Test
    public void shouldStream() throws Exception {
        final Stream<Result> stream = resultSet.stream();
        final AtomicInteger counter = new AtomicInteger(0);

        addToQueue(100, 1, true, true);

        stream.forEach(r -> counter.incrementAndGet());

        assertEquals(100, counter.get());
    }
    
    @Test
    public void shouldCallHasNextWithoutSideEffect() throws Exception {
        final Iterator itty = resultSet.iterator();
        final AtomicInteger counter = new AtomicInteger(0);

        addToQueue(100, 1, true, true);

        for (int i = 0; i < 101; i++) {
        	assertThat(itty.hasNext(), is(true));
        }
        
        while (itty.hasNext()) {
            itty.next();
            counter.incrementAndGet();
        }

        assertEquals(100, counter.get());
    }
}
