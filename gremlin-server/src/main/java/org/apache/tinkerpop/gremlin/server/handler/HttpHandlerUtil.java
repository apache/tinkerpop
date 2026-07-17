/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Meter;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Provides methods shared by the HTTP handlers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HttpHandlerUtil {
    private static final Logger logger = LoggerFactory.getLogger(HttpHandlerUtil.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    /**
     * A generic mapper to return JSON errors in specific cases.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Helper method to send errors back as JSON. Only to be used when the RequestMessage couldn't be parsed, because
     * a proper serialized ResponseMessage should be sent in that case.
     *
     * @param ctx           The netty channel context.
     * @param status        The HTTP error status code.
     * @param message       The error message to contain the body.
     * @param headers       Optional alternating header name/value pairs to set on the response.
     */
    public static void sendError(final ChannelHandlerContext ctx, final HttpResponseStatus status, final String message,
                                  final CharSequence... headers) {
        // Server errors (5xx) are logged at WARN since they signal a problem with the server itself. Client errors
        // (4xx and other non-5xx) are expected during normal operation - bad credentials, favicon probes, unsupported
        // methods, malformed requests - so they log at DEBUG to avoid flooding the log with routine client mistakes
        // while remaining available for diagnosis when needed.
        if (status.code() >= 500) {
            logger.warn("Invalid request - responding with {} and {}", status, message);
        } else if (logger.isDebugEnabled()) {
            logger.debug("Invalid request - responding with {} and {}", status, message);
        }
        errorMeter.mark();

        final ObjectNode node = mapper.createObjectNode();
        node.put("message", message);

        final FullHttpResponse response = new DefaultFullHttpResponse(
                HTTP_1_1, status, Unpooled.copiedBuffer(node.toString(), CharsetUtil.UTF_8));
        response.headers().set(CONTENT_TYPE, "application/json");
        for (int i = 0; i < headers.length; i += 2) {
            response.headers().set(headers[i], headers[i + 1]);
        }
        HttpUtil.setContentLength(response, response.content().readableBytes());

        ctx.writeAndFlush(response);
    }
}
