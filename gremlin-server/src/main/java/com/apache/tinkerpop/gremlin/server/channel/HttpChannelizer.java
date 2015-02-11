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
package com.apache.tinkerpop.gremlin.server.channel;

import com.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import com.apache.tinkerpop.gremlin.server.Graphs;
import com.apache.tinkerpop.gremlin.server.Settings;
import com.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Constructs a {@link com.apache.tinkerpop.gremlin.server.Channelizer} that exposes an HTTP/REST endpoint in Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpChannelizer extends AbstractChannelizer {
    private static final Logger logger = LoggerFactory.getLogger(HttpChannelizer.class);

    private HttpGremlinEndpointHandler httpGremlinEndpointHandler;

    @Override
    public void init(final Settings settings, final GremlinExecutor gremlinExecutor,
                     final ExecutorService gremlinExecutorService, final Graphs graphs,
                     final ScheduledExecutorService scheduledExecutorService) {
        super.init(settings, gremlinExecutor, gremlinExecutorService, graphs, scheduledExecutorService);
        httpGremlinEndpointHandler = new HttpGremlinEndpointHandler(serializers, gremlinExecutor);
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("log-io", LogLevel.DEBUG));

        pipeline.addLast("http-server", new HttpServerCodec());

        if (logger.isDebugEnabled())
            pipeline.addLast(new LoggingHandler("http-io", LogLevel.DEBUG));

        pipeline.addLast(new HttpObjectAggregator(settings.maxContentLength));
        pipeline.addLast("http-gremlin-handler", httpGremlinEndpointHandler);
    }

    @Override
    public void finalize(final ChannelPipeline pipeline) {
        pipeline.remove(PIPELINE_OP_SELECTOR);
        pipeline.remove(PIPELINE_RESULT_ITERATOR_HANDLER);
        pipeline.remove(PIPELINE_OP_EXECUTOR);
    }
}
