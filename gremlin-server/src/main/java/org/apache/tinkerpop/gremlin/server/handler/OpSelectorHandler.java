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

    public OpSelectorHandler(final Settings settings, final GraphManager graphManager, final GremlinExecutor gremlinExecutor,
                             final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
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
                throw new OpProcessorException(errorMessage, ResponseMessage.build(msg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(errorMessage).create());
            }
        } catch (OpProcessorException ope) {
            logger.warn(ope.getMessage(), ope);
            ctx.writeAndFlush(ope.getResponseMessage());
        }
    }
}
