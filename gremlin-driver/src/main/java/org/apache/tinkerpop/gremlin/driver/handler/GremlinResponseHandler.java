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
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultQueue;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Takes a map of requests pending responses and writes responses to the {@link ResultQueue} of a request
 * as the {@link ResponseMessageV4} objects are deserialized.
 */
public class GremlinResponseHandler extends SimpleChannelInboundHandler<ResponseMessageV4> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseHandler.class);
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
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessageV4 response) {
        final HttpResponseStatus statusCode = response.getStatus() == null ? HttpResponseStatus.PARTIAL_CONTENT : response.getStatus().getCode();
        final ResultQueue queue = pending.get();

        if (statusCode == HttpResponseStatus.OK || statusCode == HttpResponseStatus.PARTIAL_CONTENT) {
            final List<Object> data = response.getResult().getData();
            // unrolls the collection into individual results to be handled by the queue.
            data.forEach(item -> queue.add(new Result(item)));
        } else {
            // this is a "success" but represents no results otherwise it is an error
            if (statusCode != HttpResponseStatus.NO_CONTENT) {
                queue.markError(new ResponseException(response.getStatus().getCode(), response.getStatus().getMessage(),
                        response.getStatus().getException()));
            }
        }

        // as this is a non-PARTIAL_CONTENT code - the stream is done.
        if (statusCode != HttpResponseStatus.PARTIAL_CONTENT) {
            final ResultQueue current = pending.getAndSet(null);
            if (current != null) {
                current.markComplete(response.getStatus().getAttributes());
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

        // serialization exceptions should not close the channel - that's worth a retry
        if (!IteratorUtils.anyMatch(ExceptionUtils.getThrowableList(cause).iterator(), t -> t instanceof SerializationException))
            if (ctx.channel().isActive()) ctx.close();
    }
}
