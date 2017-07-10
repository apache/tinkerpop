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

import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.apache.tinkerpop.gremlin.server.AbstractChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler;
import org.apache.tinkerpop.gremlin.server.handler.WsAndHttpChannelizerHandler;
import org.apache.tinkerpop.gremlin.server.util.ServerGremlinExecutor;
import org.apache.tinkerpop.gremlin.server.channel.WebSocketChannelizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *A channelizer for port unification with websockets and http
 *@author Keith Lohnes lohnesk@gmail.com
 */

public class WsAndHttpChannelizer extends AbstractChannelizer {

    private static final Logger logger = LoggerFactory.getLogger(WsAndHttpChannelizer.class);

    private WsAndHttpChannelizerHandler handler;

    @Override
    public void init(final ServerGremlinExecutor<EventLoopGroup> serverGremlinExecutor) {
        super.init(serverGremlinExecutor);
        handler = new WsAndHttpChannelizerHandler();
        handler.init(serverGremlinExecutor, new HttpGremlinEndpointHandler(serializers, gremlinExecutor, graphManager, settings));
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        handler.configure(pipeline);
        pipeline.addAfter(PIPELINE_HTTP_REQUEST_DECODER, "WsAndHttpChannelizerHandler", handler);
    }
}
