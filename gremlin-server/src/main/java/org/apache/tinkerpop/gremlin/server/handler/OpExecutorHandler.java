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
package org.apache.tinkerpop.gremlin.server.handler;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class OpExecutorHandler extends SimpleChannelInboundHandler<Pair<RequestMessage, ThrowingConsumer<Context>>> {
    private static final Logger logger = LoggerFactory.getLogger(OpExecutorHandler.class);

    private final Settings settings;
    private final GraphManager graphManager;
    private final ScheduledExecutorService scheduledExecutorService;
    private final GremlinExecutor gremlinExecutor;

    public OpExecutorHandler(final Settings settings, final GraphManager graphManager, final GremlinExecutor gremlinExecutor,
                             final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext ctx, final Pair<RequestMessage, ThrowingConsumer<Context>> objects) throws Exception {
        final RequestMessage msg = objects.getValue0();
        final ThrowingConsumer<Context> op = objects.getValue1();
        final Context gremlinServerContext = new Context(msg, ctx,
                settings, graphManager, gremlinExecutor, scheduledExecutorService);
        try {
            op.accept(gremlinServerContext);
        } catch (OpProcessorException ope) {
            // Ops may choose to throw OpProcessorException or write the error ResponseMessage down the line
            // themselves
            logger.warn(ope.getMessage(), ope);
            gremlinServerContext.writeAndFlush(ope.getResponseMessage());
        } catch (Exception ex) {
            // It is possible that an unplanned exception might raise out of an OpProcessor execution. Build a general
            // error to send back to the client
            logger.warn(ex.getMessage(), ex);
            gremlinServerContext.writeAndFlush(ResponseMessage.build(msg)
                    .code(ResponseStatusCode.SERVER_ERROR)
                    .statusAttributeException(ex)
                    .statusMessage(ex.getMessage()).create());
        } finally {
            ReferenceCountUtil.release(objects);
        }
    }
}
