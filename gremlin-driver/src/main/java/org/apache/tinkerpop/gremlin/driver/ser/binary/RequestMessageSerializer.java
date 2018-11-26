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
package org.apache.tinkerpop.gremlin.driver.ser.binary;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;

import java.util.Map;
import java.util.UUID;

public class RequestMessageSerializer implements TypeSerializer<RequestMessage> {
    public RequestMessage readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) throws SerializationException {
        final int version = buffer.readByte();
        assert version >>> 31 == 1;

        final UUID id = context.readValue(buffer, UUID.class, false);
        final String op = context.readValue(buffer, String.class, false);
        final String processor = context.readValue(buffer, String.class, false);

        final RequestMessage.Builder builder = RequestMessage.build(op).overrideRequestId(id).processor(processor);

        final Map<String, Object> args = context.readValue(buffer, Map.class, false);
        args.forEach(builder::addArg);

        return builder.create();
    }

    @Override
    public ByteBuf write(RequestMessage value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        // There is no type code / information for the request message itself.
        throw new SerializationException("RequestMessageSerializer can not be written with type information");
    }

    @Override
    public ByteBuf writeValue(RequestMessage value, ByteBufAllocator allocator, GraphBinaryWriter context,
                              boolean nullable) throws SerializationException {
        return allocator.compositeBuffer(5).addComponents(true,
                // Version
                allocator.buffer(1).writeByte(0x81),
                // RequestId
                context.writeValue(value.getRequestId(), allocator, false),
                // Op
                context.writeValue(value.getOp(), allocator, false),
                // Processor
                context.writeValue(value.getProcessor(), allocator, false),
                // Args
                context.writeValue(value.getArgs(), allocator, false));
    }

    @Override
    public RequestMessage read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // There is no type code / information for the request message itself.
        throw new SerializationException("RequestMessageSerializer must not invoked with type information");
    }
}
