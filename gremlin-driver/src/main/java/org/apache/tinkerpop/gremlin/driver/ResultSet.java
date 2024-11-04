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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A {@code ResultSet} is returned from the submission of a Gremlin script to the server and represents the
 * results provided by the server.  The results from the server are streamed into the {@code ResultSet} and
 * therefore may not be available immediately.  As such, {@code ResultSet} provides access to a a number
 * of functions that help to work with the asynchronous nature of the data streaming back.  Data from results
 * is stored in an {@link Result} which can be used to retrieve the item once it is on the client side.
 * <p/>
 * Note that a {@code ResultSet} is a forward-only stream only so depending on how the methods are called and
 * interacted with, it is possible to return partial bits of the total response (e.g. calling {@link #one()} followed
 * by {@link #all()} will make it so that the {@link List} of results returned from {@link #all()} have one
 * {@link Result} missing from the total set as it was already retrieved by {@link #one}.
 * <p/>
 * This class is not thread-safe.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class ResultSet implements Iterable<Result> {
    private final ResultQueue resultQueue;
    private final ExecutorService executor;
    private final RequestMessage originalRequestMessage;
    private final Host host;

    private final CompletableFuture<Void> readCompleted;

    public ResultSet(final ResultQueue resultQueue, final ExecutorService executor,
                     final CompletableFuture<Void> readCompleted, final RequestMessage originalRequestMessage,
                     final Host host) {
        this.executor = executor;
        this.host = host;
        this.resultQueue = resultQueue;
        this.readCompleted = readCompleted;
        this.originalRequestMessage = originalRequestMessage;
    }

    public RequestMessage getOriginalRequestMessage() {
        return originalRequestMessage;
    }

    public Host getHost() {
        return host;
    }

    /**
     * Determines if all items have been returned to the client.
     */
    public boolean allItemsAvailable() {
        return readCompleted.isDone();
    }

    /**
     * Returns a future that will complete when all items have been returned from the server.
     */
    public CompletableFuture<Void> allItemsAvailableAsync() {
        // readCompleted future is always completed by Netty's event loop thread pool. To avoid blocking the event loop,
        // create a new completion stage that is completed by executor thread loop, when returning a future outside the
        // client (ie application code).
        return readCompleted.whenCompleteAsync((s,t) -> {}, executor);
    }

    /**
     * Gets the number of items available on the client.
     */
    public int getAvailableItemCount() {
        return resultQueue.size();
    }

    /**
     * Get the next {@link Result} from the stream, blocking until one is available.
     */
    public Result one() {
        final List<Result> results = some(1).join();

        assert results.size() <= 1;

        return results.size() == 1 ? results.get(0) : null;
    }

    /**
     * The returned {@link CompletableFuture} completes when the number of items specified are available.  The
     * number returned will be equal to or less than that number.  They will only be less if the stream is
     * completed and there are less than that number specified available.
     */
    public CompletableFuture<List<Result>> some(final int items) {
        return resultQueue.await(items);
    }

    /**
     * The returned {@link CompletableFuture} completes when all reads are complete for this request and the
     * entire result has been accounted for on the client. While this method is named "all" it really refers to
     * retrieving all remaining items in the set.  For large result sets it is preferred to use
     * {@link Iterator} or {@link Stream} options, as the results will be held in memory at once.
     */
    public CompletableFuture<List<Result>> all() {
        return readCompleted.thenApplyAsync(unusedInput -> {
            final List<Result> list = new ArrayList<>();
            resultQueue.drainTo(list);
            return list;
        }, executor);
    }

    /**
     * Stream items with a blocking iterator.
     */
    public Stream<Result> stream() {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator(),
                Spliterator.IMMUTABLE | Spliterator.SIZED), false);
    }

    /**
     * Returns a blocking iterator of the items streaming from the server to the client. This {@link Iterator} will
     * consume results as they arrive and leaving the {@code ResultSet} empty when complete.
     * <p/>
     * The returned {@link Iterator} does not support the {@link Iterator#remove} method.
     */
    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>() {
            private Result nextOne = null;

            @Override
            public boolean hasNext() {
                if (null == nextOne) {
                    nextOne = one();
                }
                return nextOne != null;
            }

            @Override
            public Result next() {
                if (null != nextOne || hasNext()) {
                    final Result r = nextOne;
                    nextOne = null;
                    return r;
                } else
                    throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
}
