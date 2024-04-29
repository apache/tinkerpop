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
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;

import java.util.List;

/**
 * Converts {@code HttpResponse} to a {@link ResponseMessageV4}.
 */
@ChannelHandler.Sharable
public final class HttpGremlinResponseDebugStreamDecoder extends MessageToMessageDecoder<ResponseMessageV4> {
    public HttpGremlinResponseDebugStreamDecoder() {}

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ResponseMessageV4 response, final List<Object> objects) throws Exception {
        System.out.println("HttpGremlinResponseStreamDecoder: ");
        System.out.println(response.getResult());
    }
}
