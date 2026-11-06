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

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Bounds the idle time between inbound response chunks on a per-request basis. The timeout is <em>armed</em> when a
 * request is written and rescheduled on every inbound read; it is <em>disarmed</em> once the last chunk of the
 * response arrives. This differs from a static {@link io.netty.handler.timeout.ReadTimeoutHandler} so that it never
 * fires while a pooled connection sits idle between requests (which is the concern of the idle-pool-close handler).
 * <p>
 * When the timeout elapses while awaiting a response a {@link ReadTimeoutException} is fired up the pipeline and the
 * channel is closed.
 */
public class ReadTimeoutHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(ReadTimeoutHandler.class);

    private final long timeoutMillis;

    private ScheduledFuture<?> timeoutFuture;
    private boolean awaitingResponse;
    private boolean closed;

    public ReadTimeoutHandler(final long timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        // A request is being sent - arm the timeout once the write completes so that the clock starts while we wait
        // for the server's first byte.
        promise.addListener(future -> {
            if (future.isSuccess()) {
                awaitingResponse = true;
                schedule(ctx);
            }
        });
        ctx.write(msg, promise);
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
        if (awaitingResponse) {
            if (msg instanceof LastHttpContent) {
                // the response is complete - disarm until the next request is written
                awaitingResponse = false;
                cancel();
            } else {
                // reschedule on every inbound chunk so the timeout only fires on idle gaps between chunks
                schedule(ctx);
            }
        }
        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx) throws Exception {
        cancel();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(final ChannelHandlerContext ctx) throws Exception {
        cancel();
    }

    private void schedule(final ChannelHandlerContext ctx) {
        cancel();
        if (timeoutMillis <= 0 || closed) {
            return;
        }
        timeoutFuture = ctx.executor().schedule(() -> {
            if (closed || !awaitingResponse) {
                return;
            }
            closed = true;
            logger.debug("Read timeout of {}ms exceeded while awaiting a response on channel {} - closing channel",
                    timeoutMillis, ctx.channel().id().asShortText());
            ctx.fireExceptionCaught(ReadTimeoutException.INSTANCE);
            ctx.close();
        }, timeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void cancel() {
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
            timeoutFuture = null;
        }
    }
}
