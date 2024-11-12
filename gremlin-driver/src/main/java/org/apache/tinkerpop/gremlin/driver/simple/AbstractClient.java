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
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractClient implements SimpleClient {
    protected final CallbackResponseHandler callbackResponseHandler = new CallbackResponseHandler();
    protected final EventLoopGroup group;

    public AbstractClient(final String threadPattern) {
        final BasicThreadFactory threadFactory = new BasicThreadFactory.Builder().namingPattern(threadPattern).build();
        group = new NioEventLoopGroup(1, threadFactory);
    }

    public abstract void writeAndFlush(final RequestMessage requestMessage) throws Exception;

    @Override
    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception {
        callbackResponseHandler.callback = callback;
        writeAndFlush(requestMessage);
    }

    @Override
    public List<ResponseMessage> submit(final RequestMessage requestMessage) throws Exception {
        // this is just a test client to force certain behaviors of the server. hanging tests are a pain to deal with
        // especially in travis as it's not always clear where the hang is. a few reasonable timeouts might help
        // make debugging easier when we look at logs
        return submitAsync(requestMessage).get(180, TimeUnit.SECONDS);
    }

    static class CallbackResponseHandler extends SimpleChannelInboundHandler<ResponseMessage> {
        public Consumer<ResponseMessage> callback;

        @Override
        protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final ResponseMessage response) throws Exception {
            callback.accept(response);
        }
    }
}
