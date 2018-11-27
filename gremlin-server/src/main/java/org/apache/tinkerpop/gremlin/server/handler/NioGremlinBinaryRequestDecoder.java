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

import io.netty.handler.codec.ReplayingDecoder;
import io.netty.util.CharsetUtil;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class NioGremlinBinaryRequestDecoder extends ReplayingDecoder<NioGremlinBinaryRequestDecoder.DecoderState> {
    private static final Logger logger = LoggerFactory.getLogger(NioGremlinBinaryRequestDecoder.class);
    private final Map<String, MessageSerializer> serializers;
    private int messageLength;

    public NioGremlinBinaryRequestDecoder(final Map<String, MessageSerializer> serializers) {
        super(DecoderState.MESSAGE_LENGTH);
        this.serializers = serializers;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf byteBuf, final List<Object> objects) throws Exception {
        switch (state()) {
            case MESSAGE_LENGTH:
                messageLength = byteBuf.readInt();
                checkpoint(DecoderState.MESSAGE);
            case MESSAGE:
                try {
                    final ByteBuf messageFrame = byteBuf.readBytes(messageLength);
                    final int contentTypeLength = messageFrame.readByte();
                    final ByteBuf contentTypeFrame = messageFrame.readBytes(contentTypeLength);
                    final String contentType = contentTypeFrame.toString(CharsetUtil.UTF_8);

                    // the default serializer to use has been GraphSON 1.0 to this point (which was probably bad),
                    // so maintain that for 3.3.x
                    final MessageSerializer serializer = select(contentType, ServerSerializers.DEFAULT_SERIALIZER);

                    // it's important to re-initialize these channel attributes as they apply globally to the channel. in
                    // other words, the next request to this channel might not come with the same configuration and mixed
                    // state can carry through from one request to the next
                    channelHandlerContext.channel().attr(StateKey.SESSION).set(null);
                    channelHandlerContext.channel().attr(StateKey.SERIALIZER).set(serializer);
                    channelHandlerContext.channel().attr(StateKey.USE_BINARY).set(true);

                    // subtract the contentTypeLength and the byte that held it from the full message length to
                    // figure out how long the rest of the message is
                    final int payloadLength = messageLength - 1 - contentTypeLength;
                    objects.add(serializer.deserializeRequest(messageFrame.readBytes(payloadLength)));
                } catch (SerializationException se) {
                    objects.add(RequestMessage.INVALID);
                }

                checkpoint(DecoderState.MESSAGE_LENGTH);
                break;
            default:
                throw new Error("Invalid message sent to Gremlin Server");
        }
    }

    public MessageSerializer select(final String mimeType, final MessageSerializer defaultSerializer) {
        if (logger.isWarnEnabled() && !serializers.containsKey(mimeType))
            logger.warn("Gremlin Server is not configured with a serializer for the requested mime type [{}] - using {} by default",
                    mimeType, defaultSerializer.getClass().getName());

        return serializers.getOrDefault(mimeType, defaultSerializer);
    }

    public enum DecoderState {
        MESSAGE_LENGTH,
        MESSAGE
    }
}
