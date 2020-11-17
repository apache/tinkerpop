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

import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class OpSelectorHandler extends MessageToMessageDecoder<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(OpSelectorHandler.class);

    private final Settings settings;
    private final GraphManager graphManager;

    private final GremlinExecutor gremlinExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Channelizer channelizer;

    public OpSelectorHandler(final Settings settings, final GraphManager graphManager, final GremlinExecutor gremlinExecutor,
                             final ScheduledExecutorService scheduledExecutorService, final Channelizer channelizer) {
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
        this.channelizer = channelizer;
    }

    @Override
    protected void decode(final ChannelHandlerContext ctx, final RequestMessage msg,
                          final List<Object> objects) throws Exception {
        final Context gremlinServerContext = new Context(msg, ctx, settings,
                graphManager, gremlinExecutor, this.scheduledExecutorService);
        try {
            // choose a processor to do the work based on the request message.
            final Optional<OpProcessor> processor = OpLoader.getProcessor(msg.getProcessor());

            if (processor.isPresent())
                // the processor is known so use it to evaluate the message
                objects.add(Pair.with(msg, processor.get().select(gremlinServerContext)));
            else {
                // invalid op processor selected so write back an error by way of OpProcessorException.
                final String errorMessage = String.format("Invalid OpProcessor requested [%s]", msg.getProcessor());
                throw new OpProcessorException(errorMessage, ResponseMessage.build(msg)
                        .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS)
                        .statusMessage(errorMessage).create());
            }
        } catch (OpProcessorException ope) {
            logger.warn(ope.getMessage(), ope);
            gremlinServerContext.writeAndFlush(ope.getResponseMessage());
        }
    }

    @Override
    public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
        // only need to handle this event if the idle monitor is on
        if (!channelizer.supportsIdleMonitor()) return;

        if (evt instanceof IdleStateEvent) {
            final IdleStateEvent e = (IdleStateEvent) evt;

            // if no requests (reader) then close, if no writes from server to client then ping. clients should
            // periodically ping the server, but coming from this direction allows the server to kill channels that
            // have dead clients on the other end
            if (e.state() == IdleState.READER_IDLE) {
                logger.info("Closing channel - client is disconnected after idle period of " + settings.idleConnectionTimeout + " " + ctx.channel().id().asShortText());
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE && settings.keepAliveInterval > 0) {
                logger.info("Checking channel - sending ping to client after idle period of " + settings.keepAliveInterval + " " + ctx.channel().id().asShortText());
                ctx.writeAndFlush(channelizer.createIdleDetectionMessage());
            }
        }
    }
}
