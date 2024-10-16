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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.util.AttributeKey;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultQueue;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.tinkerpop.gremlin.driver.Channelizer.HttpChannelizer.LAST_CONTENT_READ_RESPONSE;

/**
 * Takes a map of requests pending responses and writes responses to the {@link ResultQueue} of a request
 * as the {@link ResponseMessage} objects are deserialized.
 */
public class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
    public static final AttributeKey<Throwable> INBOUND_SSL_EXCEPTION = AttributeKey.valueOf("inboundSslException");
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);
    private static final AttributeKey<ResponseException> CAUGHT_EXCEPTION = AttributeKey.valueOf("caughtException");
    private final AtomicReference<ResultQueue> pending;

    public GremlinResponseHandler(final AtomicReference<ResultQueue> pending) {
        this.pending = pending;
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        // occurs when the server shuts down in a disorderly fashion, otherwise in an orderly shutdown the server
        // should fire off a close message which will properly release the driver.
        super.channelInactive(ctx);

        final ResultQueue current = pending.getAndSet(null);
        if (current != null) {
            current.markError(new IllegalStateException("Connection to server is no longer active"));
        }
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) {
        final HttpResponseStatus statusCode = response.getStatus() == null ? null : response.getStatus().getCode();
        final ResultQueue queue = pending.get();

        if ((null == statusCode) || (statusCode == HttpResponseStatus.OK)) {
            final List<Object> data = response.getResult().getData();
            final boolean bulked = channelHandlerContext.channel().attr(HttpGremlinResponseStreamDecoder.IS_BULKED).get();
            // unrolls the collection into individual results to be handled by the queue.
            if (bulked) {
                for (Iterator<Object> iter = data.iterator(); iter.hasNext(); ) {
                    final Object obj = iter.next();
                    final long bulk = (long) iter.next();
                    DefaultRemoteTraverser<Object> item = new DefaultRemoteTraverser<>(obj, bulk);
                    queue.add(new Result(item));
                }
            } else {
                data.forEach(item -> queue.add(new Result(item)));
            }

        } else {
            // this is a "success" but represents no results otherwise it is an error
            if (statusCode != HttpResponseStatus.NO_CONTENT) {
                // Save the error because there could be a subsequent HttpContent coming (probably just trailers). All
                // content should be read first before marking the queue or else this channel might get reused too early.
                channelHandlerContext.channel().attr(CAUGHT_EXCEPTION).set(
                        new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage(),
                                response.getStatus().getException())
                );
            }
        }

        // Stream is done when the last content signaling response message is read.
        if (LAST_CONTENT_READ_RESPONSE == response) {
            final ResultQueue resultQueue = pending.getAndSet(null);
            if (resultQueue != null) {
                if (null == channelHandlerContext.channel().attr(CAUGHT_EXCEPTION).get()) {
                    resultQueue.markComplete();
                } else {
                    resultQueue.markError(channelHandlerContext.channel().attr(CAUGHT_EXCEPTION).getAndSet(null));
                }
            }
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        // if this happens enough times (like the client is unable to deserialize a response) the pending
        // messages queue will not clear.  wonder if there is some way to cope with that.  of course, if
        // there are that many failures someone would take notice and hopefully stop the client.
        logger.error("Could not process the response", cause);

        final ResultQueue pendingQueue = pending.getAndSet(null);
        if (pendingQueue != null) pendingQueue.markError(cause);

        if (ExceptionHelper.getRootCause(cause) instanceof SSLException) {
            // inbound ssl error can happen with tls 1.3 because client certification auth can fail after the handshake completes
            // store the inbound ssl error so that outbound can retrieve it
            ctx.channel().attr(INBOUND_SSL_EXCEPTION).set(cause);
        }

        // serialization exceptions should not close the channel - that's worth a retry
        if (!IteratorUtils.anyMatch(ExceptionUtils.getThrowableList(cause).iterator(), t -> t instanceof SerializationException))
            if (ctx.channel().isActive()) ctx.close();
    }
}
