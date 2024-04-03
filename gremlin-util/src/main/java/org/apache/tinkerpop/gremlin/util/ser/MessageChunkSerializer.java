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
package org.apache.tinkerpop.gremlin.util.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.util.EnumSet;

public interface MessageChunkSerializer<M> extends MessageSerializer<M> {
    public ByteBuf writeHeader(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeChunk(final Object aggregate, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ByteBuf writeErrorFooter(final ResponseMessage responseMessage, final ByteBufAllocator allocator) throws SerializationException;

    public ResponseMessage readChunk(final ByteBuf byteBuf, final boolean isFirstChunk) throws SerializationException;

    public enum MessageParts {
        HEADER, DATA, FOOTER;

        public static final EnumSet<MessageParts> ALL = EnumSet.of(HEADER, DATA, FOOTER);
        public static final EnumSet<MessageParts> START = EnumSet.of(HEADER, DATA);
        public static final EnumSet<MessageParts> CHUNK = EnumSet.of(DATA);
        public static final EnumSet<MessageParts> END = EnumSet.of(DATA, FOOTER);
        public static final EnumSet<MessageParts> ERROR = EnumSet.of(FOOTER);
    }
}
