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

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.10, not replaced, use {@link WebSocketClient}.
 */
@Deprecated
public final class NioGremlinRequestEncoder extends MessageToByteEncoder<Object> {
    private boolean binaryEncoding;

    private final MessageSerializer serializer;

    public NioGremlinRequestEncoder(final boolean binaryEncoding, final MessageSerializer serializer) {
        this.binaryEncoding = binaryEncoding;
        this.serializer = serializer;
    }

    @Override
    protected void encode(final ChannelHandlerContext channelHandlerContext, final Object msg, final ByteBuf byteBuf) throws Exception {
        final RequestMessage requestMessage = (RequestMessage) msg;
        try {
            if (binaryEncoding) {
                // wrap the serialized message/payload inside of a "frame".  this works around the problem where
                // the length of the payload is not encoded into the general protocol.  that length isn't needed
                // for websockets because under that protocol, the message is wrapped in a "websocket frame". this
                // is not the optimal way to deal with this really, but it does prevent a protocol change in this
                // immediate moment trying to get the NioChannelizer working.
                final ByteBuf bytes = serializer.serializeRequestAsBinary(requestMessage, channelHandlerContext.alloc());
                byteBuf.writeInt(bytes.readableBytes());
                byteBuf.writeBytes(bytes);
            } else {
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                final byte [] bytes = textSerializer.serializeRequestAsString(requestMessage).getBytes(CharsetUtil.UTF_8);
                byteBuf.writeInt(bytes.length);
                byteBuf.writeBytes(bytes);
            }
        } catch (Exception ex) {
            throw new ResponseException(ResponseStatusCode.REQUEST_ERROR_SERIALIZATION, String.format(
                    "An error occurred during serialization of this request [%s] - it could not be sent to the server - Reason: %s",
                    requestMessage, ex));
        }
    }
}
