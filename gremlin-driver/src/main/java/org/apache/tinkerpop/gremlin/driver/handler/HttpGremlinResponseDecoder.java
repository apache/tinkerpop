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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.MessageTextSerializer;

import java.util.List;

/**
 * Converts {@code HttpResponse} to a {@link ResponseMessage}.
 */
@ChannelHandler.Sharable
public final class HttpGremlinResponseDecoder extends MessageToMessageDecoder<FullHttpResponse> {
    private final MessageSerializer<?> serializer;

    public HttpGremlinResponseDecoder(final MessageSerializer<?> serializer) {
        this.serializer = serializer;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final FullHttpResponse httpResponse, final List<Object> objects) throws Exception {
        objects.add(serializer.deserializeResponse(httpResponse.content()));
    }
}
