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

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;

/**
 * A wrapper around UnifiedChannelizer which saves and exposes the ChannelHandlerContext for testing purposes
 */
public class WsAndHttpTestChannelizer extends WsAndHttpChannelizer implements TestChannelizer {

    final ContextHandler contextHandler;

    public WsAndHttpTestChannelizer() {
        contextHandler = new ContextHandler();
    }

    @Override
    public void configure(final ChannelPipeline pipeline) {
        super.configure(pipeline);
        pipeline.addFirst(contextHandler);
    }

    public ChannelHandlerContext getMostRecentChannelHandlerContext() {
        return contextHandler.getMostRecentChannelHandlerContext();
    }

    public void resetChannelHandlerContext() {
        contextHandler.resetChannelHandlerContext();
    }
}

