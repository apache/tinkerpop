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
package org.apache.tinkerpop.gremlin.util.ser.binary;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class RequestMessageSerializer {
    private static NettyBufferFactory bufferFactory = new NettyBufferFactory();

    public RequestMessage readValue(final ByteBuf byteBuf, final GraphBinaryReader context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        final int version = buffer.readByte() & 0xff;

        if (version >>> 7 != 1) {
            // This is an indication that the request buffer was incorrectly built
            // Or the buffer offsets are wrong
            throw new SerializationException("The most significant bit should be set according to the format");
        }

        try {
            final UUID id = context.readValue(buffer, UUID.class, false);
            final String op = context.readValue(buffer, String.class, false);
            final String processor = context.readValue(buffer, String.class, false);

            final RequestMessage.Builder builder = RequestMessage.build(op).overrideRequestId(id).processor(processor);

            final Map<String, Object> args = context.readValue(buffer, Map.class, false);
            args.forEach(builder::addArg);

            return builder.create();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public void writeValue(final RequestMessage value, final ByteBuf byteBuf, final GraphBinaryWriter context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        try {
            // Version
            buffer.writeByte(GraphBinaryWriter.VERSION_BYTE);
            // RequestId
            context.writeValue(value.getRequestId(), buffer, false);
            // Op
            context.writeValue(value.getOp(), buffer, false);
            // Processor
            context.writeValue(value.getProcessor(), buffer, false);
            // Args
            context.writeValue(value.getArgs(), buffer, false);
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
