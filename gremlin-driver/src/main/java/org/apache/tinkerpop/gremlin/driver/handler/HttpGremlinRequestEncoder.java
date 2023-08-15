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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * Converts {@link RequestMessage} to a {@code HttpRequest}.
 */
@ChannelHandler.Sharable
public final class HttpGremlinRequestEncoder extends MessageToMessageEncoder<RequestMessage> {
    private final MessageSerializer<?> serializer;

    private static final ObjectMapper mapper = new ObjectMapper();

    private final UnaryOperator<FullHttpRequest> interceptor;

    public HttpGremlinRequestEncoder(final MessageSerializer<?> serializer, final UnaryOperator<FullHttpRequest> interceptor) {
        this.serializer = serializer;
        this.interceptor = interceptor;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final RequestMessage requestMessage, final List<Object> objects) throws Exception {
        try {
            final String gremlin;
            final Object gremlinStringOrBytecode = requestMessage.getArg(Tokens.ARGS_GREMLIN);

            // the gremlin key can contain a Gremlin script or bytecode. if it's bytecode we can't submit it over
            // http as such. it has to be converted to a script and we can do that with the Groovy translator.
            final boolean usesBytecode = gremlinStringOrBytecode instanceof Bytecode;
            if (usesBytecode) {
                gremlin = GroovyTranslator.of("g").translate((Bytecode) gremlinStringOrBytecode).getScript();
            } else {
                gremlin = gremlinStringOrBytecode.toString();
            }
            final byte[] payload = mapper.writeValueAsBytes(new HashMap<String,Object>() {{
                put(Tokens.ARGS_GREMLIN, gremlin);
                put(Tokens.REQUEST_ID, requestMessage.getRequestId());
                if (usesBytecode) put("op", Tokens.OPS_BYTECODE);
            }});
            final ByteBuf bb = channelHandlerContext.alloc().buffer(payload.length);
            bb.writeBytes(payload);
            final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", bb);
            request.headers().add(HttpHeaderNames.CONTENT_TYPE, "application/json");
            request.headers().add(HttpHeaderNames.CONTENT_LENGTH, payload.length);
            request.headers().add(HttpHeaderNames.ACCEPT, serializer.mimeTypesSupported()[0]);

            objects.add(interceptor.apply(request));
        } catch (Exception ex) {
            throw new ResponseException(ResponseStatusCode.REQUEST_ERROR_SERIALIZATION, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        }
    }
}
