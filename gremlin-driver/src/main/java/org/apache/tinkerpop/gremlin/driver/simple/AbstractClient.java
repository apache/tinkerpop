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
package org.apache.tinkerpop.gremlin.driver.simple;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.ReferenceCountUtil;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractClient implements SimpleClient {
    protected final CallbackResponseHandler callbackResponseHandler = new CallbackResponseHandler();
    protected final EventLoopGroup group;

    public AbstractClient(final String threadPattern) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern(threadPattern).build();
        // TODO: Use Epoll if available
        group = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(), threadFactory);
    }

    public abstract void writeAndFlush(final RequestMessage requestMessage) throws Exception;

    @Override
    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception {
        callbackResponseHandler.callback = callback;
        writeAndFlush(requestMessage);
    }

    @Override
    public List<ResponseMessage> submit(final RequestMessage requestMessage) throws Exception {
        return submitAsync(requestMessage).get();
    }

    @Override
    public CompletableFuture<List<ResponseMessage>> submitAsync(final RequestMessage requestMessage) throws Exception {
        final List<ResponseMessage> results = new ArrayList<>();
        final CompletableFuture<List<ResponseMessage>> f = new CompletableFuture<>();
        callbackResponseHandler.callback = response -> {
            if (f.isDone())
                throw new RuntimeException("A terminating message was already encountered - no more messages should have been received");

            results.add(response);

            // check if the current message is terminating - if it is then we can mark complete
            if (!response.getStatus().getCode().equals(ResponseStatusCode.PARTIAL_CONTENT)) {
                f.complete(results);
            }
        };

        writeAndFlush(requestMessage);

        return f;
    }

    static class CallbackResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        public Consumer<ResponseMessage> callback;

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            try {
                callback.accept(response);
            } finally {
                ReferenceCountUtil.release(response);
            }
        }
    }
}
