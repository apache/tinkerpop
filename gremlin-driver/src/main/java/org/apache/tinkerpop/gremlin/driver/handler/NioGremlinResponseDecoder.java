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

import io.netty.handler.codec.ReplayingDecoder;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.10, not replaced, use {@link WebSocketClient}.
 */
@Deprecated
public final class NioGremlinResponseDecoder extends ReplayingDecoder<NioGremlinResponseDecoder.DecoderState> {
    private final MessageSerializer serializer;
    private int messageLength;

    public NioGremlinResponseDecoder(final MessageSerializer serializer) {
        super(DecoderState.MESSAGE_LENGTH);
        this.serializer = serializer;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final ByteBuf byteBuf, final List<Object> objects) throws Exception {
        switch (state()) {
            case MESSAGE_LENGTH:
                messageLength = byteBuf.readInt();
                checkpoint(DecoderState.MESSAGE);
            case MESSAGE:
                final ByteBuf messageFrame = byteBuf.readBytes(messageLength);
                objects.add(serializer.deserializeResponse(messageFrame));
                checkpoint(DecoderState.MESSAGE_LENGTH);
                break;
            default:
                throw new Error("Invalid message received from Gremlin Server");
        }
    }

    public enum DecoderState {
        MESSAGE_LENGTH,
        MESSAGE
    }
}
