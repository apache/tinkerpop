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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.junit.Test;

import static io.netty.util.CharsetUtil.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link HttpGremlinEndpointHandler#exceptionCaught}. When no {@link HttpResponseCoordinator} has been
 * published for the request, the error came from an upstream handler before the endpoint ran (no response started, no
 * serializer necessarily negotiated), so the handler must fall back to a self-contained {@code sendError}.
 */
public class HttpGremlinEndpointHandlerTest {

    // exceptionCaught touches none of the handler's constructor dependencies, so nulls are sufficient here.
    private static HttpGremlinEndpointHandler newHandler() {
        return new HttpGremlinEndpointHandler(null, null, null, null);
    }

    @Test
    public void exceptionCaughtFallsBackToSendErrorWhenNoCoordinator() {
        // No RESPONSE_COORDINATOR is set: this models an error from an upstream handler before the endpoint ran, where
        // no response has started (and possibly no serializer was negotiated). The handler must produce a single,
        // self-contained 500 response, preserving the pre-coordinator behavior.
        final EmbeddedChannel channel = new EmbeddedChannel(newHandler());

        channel.pipeline().fireExceptionCaught(new RuntimeException("boom"));

        final FullHttpResponse response = channel.readOutbound();
        assertEquals(500, response.status().code());
        assertEquals("application/json", response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        assertTrue(response.content().toString(UTF_8).contains("boom"));
        assertNull("only a single full response should be written", channel.readOutbound());

        ReferenceCountUtil.release(response);
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldMapInterruptToTransactionTimeoutWhenClosedByLifetimeCap() {
        // When the lifetime cap interrupts an operation, it first flags the request Context, so the interrupt that
        // unwinds the operation must be reported as a transaction timeout (504), not the generic evaluation timeout.
        final RequestMessage message = RequestMessage.build("g.V()").create();
        final Context ctx = mock(Context.class);
        when(ctx.isClosedByLifetimeCap()).thenReturn(true);
        when(ctx.getTransactionId()).thenReturn("tx-1234");

        final GremlinError error = newHandler().formErrorResponseMessage(
                new TraversalInterruptedException(), message, ctx);

        assertEquals(HttpResponseStatus.GATEWAY_TIMEOUT, error.getCode());
        assertEquals("TransactionException", error.getException());
        assertTrue(error.getMessage().contains("tx-1234"));
    }

    @Test
    public void shouldMapInterruptToEvaluationTimeoutWhenNotClosedByLifetimeCap() {
        // An ordinary evaluation-timeout interrupt (cap flag unset) must keep the existing 500 timeout behavior.
        final RequestMessage message = RequestMessage.build("g.V()").create();
        final Context ctx = mock(Context.class);
        when(ctx.isClosedByLifetimeCap()).thenReturn(false);

        final GremlinError error = newHandler().formErrorResponseMessage(
                new InterruptedException(), message, ctx);

        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, error.getCode());
        assertEquals("ServerTimeoutExceededException", error.getException());
    }
}
