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
package org.apache.tinkerpop.gremlin.server.channel;

import io.netty.channel.ChannelPipeline;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.Channelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.UnifiedHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsAndHttpChannelizerHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.server.handler.SessionExecutor;

import java.util.concurrent.ExecutorService;

/**
 * A {@link Channelizer} that supports websocket and HTTP requests and does so with the most streamlined processing
 * model for Gremlin Server introduced with 3.5.0.
 */
public class UnifiedChannelizer extends AbstractChannelizer {

    private WsAndHttpChannelizerHandler wsAndHttpChannelizerHandler;
    private UnifiedHandler unifiedHandler;
    protected static final String PIPELINE_UNIFIED = "unified";

    @Override
    public void init(final ServerGremlinExecutor serverGremlinExecutor) {
        final ExecutorService underlyingExecutorServiceForGremlin = serverGremlinExecutor.getGremlinExecutor().getExecutorService();
        if (!(underlyingExecutorServiceForGremlin instanceof SessionExecutor)) {
            throw new IllegalStateException(String.format(
                    "The %s requires use of a %s for the GremlinExecutor but a %s was provided instead",
                    UnifiedChannelizer.class.getSimpleName(), SessionExecutor.class.getName(), underlyingExecutorServiceForGremlin.getClass().getName()));
        }

        super.init(serverGremlinExecutor);

        wsAndHttpChannelizerHandler = new WsAndHttpChannelizerHandler();
        wsAndHttpChannelizerHandler.init(serverGremlinExecutor, new HttpGremlinEndpointHandler(serializers, gremlinExecutor, graphManager, settings));

        // these handlers don't share any state and can thus be initialized once per pipeline
        unifiedHandler = new UnifiedHandler(settings, graphManager, gremlinExecutor, scheduledExecutorService, this);
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        wsAndHttpChannelizerHandler.configure(pipeline);
        pipeline.addAfter(PIPELINE_HTTP_REQUEST_DECODER, "WsAndHttpChannelizerHandler", wsAndHttpChannelizerHandler);
    }

    @Override
    public void finalize(final ChannelPipeline pipeline) {
        super.finalize(pipeline);

        // currently the AbstractChannelizer adds the following handlers which prior to 3.5.0 were essentially
        // required by any Gremlin-processing pipeline you could think of because they provided the functionality
        // of the OpProcessor infrastructure. the OpProcessor infrastructure is on its way to deprecation after
        // TINKERPOP-2245 which introduced this channelizer implementation. since AbstractChannelizer does a nice
        // job of rigging up the rest of the pipeline it seemed to make sense to leave it unchanged to ensure it
        // does not break anyone's channelizers that may depend on it (including TinkerPop's and simply remove
        // those bits here. in the future, when we remove OpProcessor stuff completely we can clean this up.
        pipeline.remove(PIPELINE_OP_SELECTOR);
        pipeline.remove(PIPELINE_OP_EXECUTOR);

        pipeline.addLast(PIPELINE_UNIFIED, unifiedHandler);
    }

    public UnifiedHandler getUnifiedHandler() {
        return unifiedHandler;
    }

    @Override
    public boolean supportsIdleMonitor() {
        return true;
    }

    @Override
    public Object createIdleDetectionMessage() {
        return wsAndHttpChannelizerHandler.getWsChannelizer().createIdleDetectionMessage();
    }
}
