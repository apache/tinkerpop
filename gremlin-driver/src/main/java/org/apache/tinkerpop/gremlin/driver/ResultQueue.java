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

import org.apache.tinkerpop.gremlin.driver.handler.GremlinResponseHandler;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A queue of incoming {@link Result} objects.  The queue is updated by the {@link GremlinResponseHandler}
 * until a response terminator is identified.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public final class ResultQueue {

    private final LinkedBlockingQueue<Result> resultLinkedBlockingQueue;

    private final Object aggregatedResult = null;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final CompletableFuture<Void> readComplete;

    private final Queue<Pair<CompletableFuture<List<Result>>,Integer>> waiting = new ConcurrentLinkedQueue<>();

    public ResultQueue(final LinkedBlockingQueue<Result> resultLinkedBlockingQueue, final CompletableFuture<Void> readComplete) {
        this.resultLinkedBlockingQueue = resultLinkedBlockingQueue;
        this.readComplete = readComplete;
    }

    /**
     * Adds a {@link Result} to the queue which will be later read by the {@link ResultSet}.
     *
     * @param result a return value from the {@link Traversal} or script submitted for execution
     */
    public void add(final Result result) {
        this.resultLinkedBlockingQueue.offer(result);
        tryDrainNextWaiting(false);
    }

    public CompletableFuture<List<Result>> await(final int items) {
        final CompletableFuture<List<Result>> result = new CompletableFuture<>();
        waiting.add(Pair.with(result, items));

        tryDrainNextWaiting(false);

        return result;
    }

    public int size() {
        if (error.get() != null) throw new RuntimeException(error.get());
        return this.resultLinkedBlockingQueue.size();
    }

    public boolean isEmpty() {
        if (error.get() != null) throw new RuntimeException(error.get());
        return this.size() == 0;
    }

    public boolean isComplete() {
        return readComplete.isDone();
    }

    void drainTo(final Collection<Result> collection) {
        if (error.get() != null) throw new RuntimeException(error.get());
        resultLinkedBlockingQueue.drainTo(collection);
    }

     public void markComplete() {
        // if there was some aggregation performed in the queue then the full object is hanging out waiting to be
        // added to the ResultSet
        if (aggregatedResult != null)
            add(new Result(aggregatedResult));

        this.readComplete.complete(null);

        this.drainAllWaiting();
    }

    public void markError(final Throwable throwable) {
        error.set(throwable);
        this.readComplete.completeExceptionally(throwable);
        this.drainAllWaiting();
    }

    /**
     * Completes the next waiting future if there is one.
     */
    private synchronized void tryDrainNextWaiting(final boolean force) {
        // need to peek because the number of available items needs to be >= the expected size for that future. if not
        // it needs to keep waiting
        final Pair<CompletableFuture<List<Result>>, Integer> nextWaiting = waiting.peek();
        if (nextWaiting != null && (force || (resultLinkedBlockingQueue.size() >= nextWaiting.getValue1() || readComplete.isDone()))) {
            final int items = nextWaiting.getValue1();
            final CompletableFuture<List<Result>> future = nextWaiting.getValue0();
            final List<Result> results = new ArrayList<>(items);
            resultLinkedBlockingQueue.drainTo(results, items);

            // it's important to check for error here because a future may have already been queued in "waiting" prior
            // to the first response back from the server. if that happens, any "waiting" futures should be completed
            // exceptionally otherwise it will look like success.
            if (null == error.get())
                future.complete(results);
            else
                future.completeExceptionally(error.get());

            waiting.remove(nextWaiting);
        }
    }

    /**
     * Completes all remaining futures.
     */
    private void drainAllWaiting() {
        while (!waiting.isEmpty()) {
            tryDrainNextWaiting(true);
        }
    }
}
