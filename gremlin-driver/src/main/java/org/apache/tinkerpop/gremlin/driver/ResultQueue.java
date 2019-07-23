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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A queue of incoming {@link Result} objects.  The queue is updated by the {@link Handler.GremlinResponseHandler}
 * until a response terminator is identified.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
final class ResultQueue {

    private final LinkedBlockingQueue<Result> resultLinkedBlockingQueue;

    private Object aggregatedResult = null;

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final CompletableFuture<Void> readComplete;

    private final Queue<Pair<CompletableFuture<List<Result>>,Integer>> waiting = new ConcurrentLinkedQueue<>();

    private Map<String,Object> statusAttributes = null;

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

    /**
     * Adds a side-effect to the queue which may later be read by the {@link ResultSet}. Note that the side-effect
     * is only returned when a {@link Traversal} is submitted and refers to the side-effects defined in that traversal.
     * A "script" will not return side-effects.
     *
     * @param aggregateTo the value of the {@link ResponseMessage} metadata for {@link Tokens#ARGS_AGGREGATE_TO}.
     * @param sideEffectValue the value of the side-effect itself
     * @deprecated As of release 3.3.8, not directly replaced in the protocol as side-effect retrieval after
     * traversal iteration is not being promoted anymore as a feature.
     */
    @Deprecated
    public void addSideEffect(final String aggregateTo, final Object sideEffectValue) {
        switch (aggregateTo) {
            case Tokens.VAL_AGGREGATE_TO_BULKSET:
                if (!(sideEffectValue instanceof Traverser.Admin))
                    throw new IllegalStateException(String.format("Side-effect value %s is a %s which does not aggregate to %s",
                            sideEffectValue, sideEffectValue.getClass().getSimpleName(), aggregateTo));

                if (null == aggregatedResult) aggregatedResult = new BulkSet();

                final BulkSet<Object> bs = validate(aggregateTo, BulkSet.class);
                final Traverser.Admin traverser = (Traverser.Admin) sideEffectValue;
                bs.add(traverser.get(), traverser.bulk());
                break;
            case Tokens.VAL_AGGREGATE_TO_LIST:
                if (null == aggregatedResult) aggregatedResult = new ArrayList();
                final List<Object> list = validate(aggregateTo, List.class);
                list.add(sideEffectValue);
                break;
            case Tokens.VAL_AGGREGATE_TO_SET:
                if (null == aggregatedResult) aggregatedResult = new HashSet();
                final Set<Object> set = validate(aggregateTo, Set.class);
                set.add(sideEffectValue);
                break;
            case Tokens.VAL_AGGREGATE_TO_MAP:
                if (!(sideEffectValue instanceof Map.Entry) && !(sideEffectValue instanceof Map))
                    throw new IllegalStateException(String.format("Side-effect value %s is a %s which does not aggregate to %s",
                            sideEffectValue, sideEffectValue.getClass().getSimpleName(), aggregateTo));

                // some serialization formats (e.g. graphson) may deserialize a Map.Entry to a Map with a single entry
                if (sideEffectValue instanceof Map && ((Map) sideEffectValue).size() != 1)
                    throw new IllegalStateException(String.format("Side-effect value %s is a %s which does not aggregate to %s as it is a Map that does not have one entry",
                            sideEffectValue, sideEffectValue.getClass().getSimpleName(), aggregateTo));

                if (null == aggregatedResult) aggregatedResult =  new HashMap();

                final Map<Object,Object > m = validate(aggregateTo, Map.class);
                final Map.Entry entry = sideEffectValue instanceof Map.Entry ?
                        (Map.Entry) sideEffectValue : (Map.Entry) ((Map) sideEffectValue).entrySet().iterator().next();
                m.put(entry.getKey(), entry.getValue());
                break;
            case Tokens.VAL_AGGREGATE_TO_NONE:
                if (null == aggregatedResult) aggregatedResult = sideEffectValue;
                break;
            default:
                throw new IllegalStateException(String.format("%s is an invalid value for %s", aggregateTo, Tokens.ARGS_AGGREGATE_TO));
        }
    }

    private <V> V validate(final String aggregateTo, final Class<?> expected) {
        if (!(expected.isAssignableFrom(aggregatedResult.getClass())))
            throw new IllegalStateException(String.format("Side-effect \"%s\" contains the type %s that is not acceptable for %s",
                    aggregatedResult.getClass().getSimpleName(), aggregateTo));

        return (V) aggregatedResult;
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

    void markComplete(final Map<String,Object> statusAttributes) {
        // if there was some aggregation performed in the queue then the full object is hanging out waiting to be
        // added to the ResultSet
        if (aggregatedResult != null)
            add(new Result(aggregatedResult));

        this.statusAttributes = null == statusAttributes ? Collections.emptyMap() : statusAttributes;

        this.readComplete.complete(null);

        this.drainAllWaiting();
    }

    void markError(final Throwable throwable) {
        error.set(throwable);
        this.readComplete.completeExceptionally(throwable);
        this.drainAllWaiting();
    }

    Map<String,Object> getStatusAttributes() {
        return statusAttributes;
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
