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
package org.apache.tinkerpop.gremlin.driver.ser;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.ser.binary.*;

public class GraphBinaryMessageSerializerV1d0 extends AbstractMessageSerializer {
    private static final String MIME_TYPE = SerTokens.MIME_GRAPHBINARY_V1D0;

    private final GraphBinaryReader reader;
    private final GraphBinaryWriter writer;
    private final RequestMessageSerializer requestSerializer;
    private final ResponseMessageSerializer responseSerializer;

    /**
     * Creates a new instance of the message serializer using the default type serializers.
     */
    public GraphBinaryMessageSerializerV1d0() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryMessageSerializerV1d0(TypeSerializerRegistry registry) {
        reader = new GraphBinaryReader(registry);
        writer = new GraphBinaryWriter(registry);

        requestSerializer = new RequestMessageSerializer();
        responseSerializer = new ResponseMessageSerializer();
    }

    @Override
    public ByteBuf serializeResponseAsBinary(ResponseMessage responseMessage, ByteBufAllocator allocator) throws SerializationException {
        return responseSerializer.writeValue(responseMessage, allocator, writer, false);
    }

    @Override
    public ByteBuf serializeRequestAsBinary(RequestMessage requestMessage, ByteBufAllocator allocator) throws SerializationException {
        return requestSerializer.writeValue(requestMessage, allocator, writer);
    }

    @Override
    public RequestMessage deserializeRequest(ByteBuf msg) throws SerializationException {
        return requestSerializer.readValue(msg, reader);
    }

    @Override
    public ResponseMessage deserializeResponse(ByteBuf msg) throws SerializationException {
        return responseSerializer.readValue(msg, reader, false);
    }

    @Override
    public String[] mimeTypesSupported() {
        return new String[] { MIME_TYPE };
    }
}
