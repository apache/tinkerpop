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

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutException;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

public class ReadTimeoutHandlerTest {

    private static class CaughtExceptionHandler extends ChannelInboundHandlerAdapter {
        final AtomicReference<Throwable> caught = new AtomicReference<>();

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            caught.set(cause);
        }
    }

    @Test
    public void shouldFireReadTimeoutWhenNoResponseArrives() {
        final CaughtExceptionHandler errorHandler = new CaughtExceptionHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(new ReadTimeoutHandler(50), errorHandler);

        // simulate a request being written, which arms the timeout
        channel.writeOutbound("request");

        // advance the embedded scheduler past the timeout without any inbound read
        channel.runScheduledPendingTasks();
        try {
            Thread.sleep(80);
        } catch (InterruptedException ignored) {
        }
        channel.runScheduledPendingTasks();

        assertTrue(errorHandler.caught.get() instanceof ReadTimeoutException);
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldNotFireWhenResponseCompletes() {
        final CaughtExceptionHandler errorHandler = new CaughtExceptionHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(new ReadTimeoutHandler(50), errorHandler);

        channel.writeOutbound("request");

        // a complete response disarms the timeout
        channel.writeInbound(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
        channel.writeInbound(new DefaultLastHttpContent());

        channel.runScheduledPendingTasks();
        try {
            Thread.sleep(80);
        } catch (InterruptedException ignored) {
        }
        channel.runScheduledPendingTasks();

        assertNull(errorHandler.caught.get());
        channel.finishAndReleaseAll();
    }

    @Test
    public void shouldNotArmWhenTimeoutIsZero() {
        final CaughtExceptionHandler errorHandler = new CaughtExceptionHandler();
        final EmbeddedChannel channel = new EmbeddedChannel(new ReadTimeoutHandler(0), errorHandler);

        channel.writeOutbound("request");
        channel.runScheduledPendingTasks();

        assertFalse(errorHandler.caught.get() instanceof ReadTimeoutException);
        channel.finishAndReleaseAll();
    }
}
