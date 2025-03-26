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

import com.codahale.metrics.MetricRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpMessage;
import io.netty.util.AttributeKey;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpUserAgentHandler extends ChannelInboundHandlerAdapter {
    /**
     * This constant caps the number of unique user agents which will be tracked in the metrics. Any new unique
     * user agents will be replaced with "other" in the metrics after this cap has been reached.
     */
    private static final int MAX_USER_AGENT_METRICS = 10000;

    private static final Logger logger = LoggerFactory.getLogger(HttpUserAgentHandler.class);

    static final String USER_AGENT_HEADER_NAME = "User-Agent";

    public static final AttributeKey<String> USER_AGENT_ATTR_KEY = AttributeKey.valueOf(USER_AGENT_HEADER_NAME);

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof FullHttpMessage) {
            final FullHttpMessage request = (FullHttpMessage) msg;
            if (request.headers().contains(USER_AGENT_HEADER_NAME)) {
                final String userAgent = request.headers().get(USER_AGENT_HEADER_NAME);
                ctx.channel().attr(USER_AGENT_ATTR_KEY).set(userAgent);
                logger.debug("New Connection on channel [{}] with user agent [{}]", ctx.channel().id().asShortText(), userAgent);

                String metricName = MetricRegistry.name(GremlinServer.class, "user-agent", userAgent);

                // This check is to address a concern that an attacker may try to fill the server's memory with a very
                // large number of unique user agents. For this reason the user agent is replaced with "other"
                // for the purpose of metrics if this cap is ever exceeded and the user agent is not already being tracked.
                if (MetricManager.INSTANCE.getCounterSize() > MAX_USER_AGENT_METRICS &&
                        !MetricManager.INSTANCE.contains(metricName)) {
                    metricName = MetricRegistry.name(GremlinServer.class, "user-agent", "other");
                }
                MetricManager.INSTANCE.getCounter(metricName).inc();
            } else {
                logger.debug("New Connection on channel [{}] with no user agent provided", ctx.channel().id().asShortText());
            }
        }
        ctx.fireChannelRead(msg);
    }
}
