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

import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.util.concurrent.ScheduledExecutorService;

/**
 * A {@code SessionTask} equates to a particular incoming request to the {@link UnifiedHandler} and is analogous to
 * a {@link Context} in the {@link OpProcessor} approach to handling requests to the server.
 */
public class SessionTask extends Context {
    public SessionTask(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                       final Settings settings, final GraphManager graphManager,
                       final GremlinExecutor gremlinExecutor,
                       final ScheduledExecutorService scheduledExecutorService) {
        super(requestMessage, ctx, settings, graphManager, gremlinExecutor, scheduledExecutorService);
    }
}
