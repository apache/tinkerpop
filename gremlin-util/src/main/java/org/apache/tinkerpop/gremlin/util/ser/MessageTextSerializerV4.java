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
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

/**
 * An extension to the MessageTextSerializer interface that is used for serializing RequestMessageV4.
 */
public interface MessageTextSerializerV4<M> extends MessageTextSerializer<M> {
    /**
     * Serialize a {@link ResponseMessage} to a Netty {@code ByteBuf}.
     *
     * @param requestMessage The V4 request message to serialize to bytes.
     * @param allocator      The Netty allocator for the {@code ByteBuf} to return back.
     */
    public ByteBuf serializeRequestMessageV4(final RequestMessageV4 requestMessage, final ByteBufAllocator allocator) throws SerializationException;

    /**
     * Deserialize a Netty {@code ByteBuf} into a {@link RequestMessageV4}.
     */
    public RequestMessageV4 deserializeRequestMessageV4(final ByteBuf msg) throws SerializationException;
}
