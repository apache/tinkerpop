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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Drives {@link GremlinResponseHandler#channelRead0} directly through an {@link EmbeddedChannel}, asserting the real
 * effects the handler has on the pending {@link ResultSet} for each status/branch, without a live server.
 */
public class GremlinResponseHandlerTest {

    private ExecutorService executor;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardown() {
        executor.shutdownNow();
    }

    private ResultSet newResultSet() {
        return new ResultSet(executor, RequestMessage.build("g.V()").create(), null);
    }

    private EmbeddedChannel newChannel(final AtomicReference<ResultSet> pending, final Runnable onComplete,
                                       final boolean streaming) {
        return new EmbeddedChannel(new GremlinResponseHandler(pending, onComplete, streaming));
    }

    /**
     * A bulked response lays data out as [obj, bulk, obj, bulk, ...] and must be unrolled into
     * {@link DefaultRemoteTraverser}-backed results that carry the reported bulk.
     */
    @Test
    public void shouldUnrollBulkedResultsHonoringBulk() {
        final ResultSet rs = newResultSet();
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final EmbeddedChannel channel = newChannel(pending, () -> {}, false);
        channel.attr(HttpGremlinResponseStreamDecoder.IS_BULKED).set(true);

        final ResponseMessage msg = ResponseMessage.build().code(HttpResponseStatus.OK)
                .result(Arrays.asList("x", 2L, "y", 3L)).create();
        channel.writeInbound(msg);

        // two (value,bulk) pairs collapse into two results
        assertEquals(2, rs.getAvailableItemCount());

        final Result first = rs.one();
        assertTrue(first.getObject() instanceof DefaultRemoteTraverser);
        final DefaultRemoteTraverser<?> firstTraverser = (DefaultRemoteTraverser<?>) first.getObject();
        assertEquals("x", firstTraverser.get());
        assertEquals(2L, firstTraverser.bulk());

        final Result second = rs.one();
        assertTrue(second.getObject() instanceof DefaultRemoteTraverser);
        final DefaultRemoteTraverser<?> secondTraverser = (DefaultRemoteTraverser<?>) second.getObject();
        assertEquals("y", secondTraverser.get());
        assertEquals(3L, secondTraverser.bulk());

        channel.finishAndReleaseAll();
    }

    /**
     * An error status is buffered until end-of-stream; the following LAST_CONTENT signal marks the
     * {@link ResultSet} with that error, clears the pending reference, and runs the completion callback.
     */
    @Test
    public void shouldMarkErrorOnErrorStatusFollowedByLastContent() throws Exception {
        final ResultSet rs = newResultSet();
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final EmbeddedChannel channel = newChannel(pending, () -> completed.set(true), false);

        final ResponseMessage error = ResponseMessage.build()
                .code(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                .statusMessage("boom").create();
        channel.writeInbound(error);

        // the error is only buffered until all content is read - nothing should be finalized yet
        assertFalse(completed.get());
        assertFalse(rs.allItemsAvailable());
        assertEquals(rs, pending.get());

        channel.writeInbound(LAST_CONTENT_READ_RESPONSE);

        assertTrue(completed.get());
        assertNull(pending.get());

        try {
            rs.all().get(5, TimeUnit.SECONDS);
            fail("Expected the ResultSet to complete exceptionally with the buffered error");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof ResponseException);
            final ResponseException re = (ResponseException) e.getCause();
            assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, re.getResponseStatusCode());
            assertEquals("boom", re.getMessage());
        }

        channel.finishAndReleaseAll();
    }

    /**
     * An OK non-bulked response adds one {@link Result} per data item in order and does not complete the stream
     * until the LAST_CONTENT signal arrives; that signal then completes the {@link ResultSet} cleanly, clears the
     * pending reference, runs the completion callback, and preserves the data in order.
     */
    @Test
    public void shouldAddResultsInOrderThenCompleteOnLastContent() throws Exception {
        final ResultSet rs = newResultSet();
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final EmbeddedChannel channel = newChannel(pending, () -> completed.set(true), false);
        channel.attr(HttpGremlinResponseStreamDecoder.IS_BULKED).set(false);

        // one Result per data item is added, but with no LAST_CONTENT yet the stream is not finished
        channel.writeInbound(ResponseMessage.build().code(HttpResponseStatus.OK)
                .result(Arrays.asList("a", "b", "c")).create());
        assertEquals(3, rs.getAvailableItemCount());
        assertFalse(completed.get());
        assertFalse(rs.allItemsAvailable());

        // the LAST_CONTENT signal completes the ResultSet, clears pending, runs the callback, and preserves order
        channel.writeInbound(LAST_CONTENT_READ_RESPONSE);
        assertTrue(completed.get());
        assertNull(pending.get());
        assertTrue(rs.allItemsAvailable());

        final List<Result> all = rs.all().get(5, TimeUnit.SECONDS);
        assertEquals(3, all.size());
        assertEquals("a", all.get(0).getString());
        assertEquals("b", all.get(1).getString());
        assertEquals("c", all.get(2).getString());

        channel.finishAndReleaseAll();
    }

    /**
     * A NO_CONTENT status is a "success with no results" and must not record an error. A subsequent
     * LAST_CONTENT signal then completes the stream cleanly.
     */
    @Test
    public void shouldNotRecordErrorOnNoContentStatus() throws Exception {
        final ResultSet rs = newResultSet();
        final AtomicReference<ResultSet> pending = new AtomicReference<>(rs);
        final AtomicBoolean completed = new AtomicBoolean(false);
        final EmbeddedChannel channel = newChannel(pending, () -> completed.set(true), false);

        // a distinct NO_CONTENT message (not the LAST_CONTENT_READ_RESPONSE sentinel) must not buffer an error
        channel.writeInbound(ResponseMessage.build().code(HttpResponseStatus.NO_CONTENT).create());
        assertFalse(completed.get());

        channel.writeInbound(LAST_CONTENT_READ_RESPONSE);

        assertTrue(completed.get());
        assertNull(pending.get());
        assertTrue(rs.allItemsAvailable());

        // completes normally with no results and no error
        final List<Result> all = rs.all().get(5, TimeUnit.SECONDS);
        assertTrue(all.isEmpty());

        channel.finishAndReleaseAll();
    }
}
