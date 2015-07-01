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

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A queue of incoming {@link ResponseMessage} objects.  The queue is updated by the
 * {@link Handler.GremlinResponseHandler} until a response terminator is identified.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class ResultQueue {

    private final LinkedBlockingQueue<Result> resultLinkedBlockingQueue;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final CompletableFuture<Void> readComplete;

    private final Queue<Pair<CompletableFuture<List<Result>>,Integer>> waiting = new ConcurrentLinkedQueue<>();

    /**
     * Tracks the state of the "waiting" queue and whether or not results have been drained through it on
     * read complete.  If they are then no additional "waiting" is required.
     */
    private final AtomicBoolean flushed = new AtomicBoolean(false);

    public ResultQueue(final LinkedBlockingQueue<Result> resultLinkedBlockingQueue, final CompletableFuture<Void> readComplete) {
        this.resultLinkedBlockingQueue = resultLinkedBlockingQueue;
        this.readComplete = readComplete;
    }

    public void add(final Result result) {
        this.resultLinkedBlockingQueue.offer(result);

        final Pair<CompletableFuture<List<Result>>, Integer> nextWaiting = waiting.peek();
        if (nextWaiting != null && (resultLinkedBlockingQueue.size() >= nextWaiting.getValue1() || readComplete.isDone())) {
            internalDrain(nextWaiting.getValue1(), nextWaiting.getValue0(), resultLinkedBlockingQueue);
            waiting.remove(nextWaiting);
        }
    }

    public CompletableFuture<List<Result>> await(final int items) {
        final CompletableFuture<List<Result>> result = new CompletableFuture<>();
        if (size() >= items || readComplete.isDone()) {
            // items are present so just drain to requested size if possible then complete it
            internalDrain(items, result, resultLinkedBlockingQueue);
        } else {
            // not enough items in the result queue so save this for callback later when the results actually arrive.
            // only necessary to "wait" if we're not in the act of flushing already, in which case, no more waiting
            // for additional results should be allowed.
            if (flushed.get()) {
                // just drain since we've flushed already
                internalDrain(items, result, resultLinkedBlockingQueue);
            } else {
                waiting.add(Pair.with(result, items));
            }
        }

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

    public void drainTo(final Collection<Result> collection) {
        if (error.get() != null) throw new RuntimeException(error.get());
        resultLinkedBlockingQueue.drainTo(collection);
    }

    void markComplete() {
        this.readComplete.complete(null);
        this.flushWaiting();
    }

    void markError(final Throwable throwable) {
        error.set(throwable);

        // unsure if this should really complete exceptionally rather than just complete.
        this.readComplete.complete(null);
        this.flushWaiting();
    }

    private void flushWaiting() {
        while (waiting.peek() != null) {
            final Pair<CompletableFuture<List<Result>>, Integer> nextWaiting = waiting.poll();
            internalDrain(nextWaiting.getValue1(), nextWaiting.getValue0(), resultLinkedBlockingQueue);
        }

        flushed.set(true);
    }

    private static void internalDrain(int items, CompletableFuture<List<Result>> result, LinkedBlockingQueue<Result> resultLinkedBlockingQueue) {
        final List<Result> results = new ArrayList<>(items);
        resultLinkedBlockingQueue.drainTo(results, items);
        result.complete(results);
    }
}
