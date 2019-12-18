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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;

/**
 * Encodes {@code ByteBuf} and {@code String} values to bytes to be written over NIO.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.10, not replaced, use {@link WebSocketClient}.
 */
@Deprecated
@ChannelHandler.Sharable
public class NioGremlinResponseFrameEncoder extends MessageToByteEncoder<Frame> {
    @Override
    protected void encode(final ChannelHandlerContext ctx, final Frame frame, final ByteBuf byteBuf) throws Exception {
        if (frame.getMsg() instanceof ByteBuf) {
            final ByteBuf bytes = (ByteBuf) frame.getMsg();
            byteBuf.writeInt(bytes.capacity());
            byteBuf.writeBytes(bytes);
            bytes.release();
        } else if (frame.getMsg() instanceof String) {
            final byte [] bytes = ((String) frame.getMsg()).getBytes(CharsetUtil.UTF_8);
            byteBuf.writeInt(bytes.length);
            byteBuf.writeBytes(bytes);
        }
    }
}
