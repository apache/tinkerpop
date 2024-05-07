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
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseResultV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusV4;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import java.io.IOException;
import java.util.Map;

public class ResponseMessageSerializerV4 {
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();

    public ResponseMessageV4 readValue(final ByteBuf byteBuf, final GraphBinaryReader context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);
        final int version = buffer.readByte() & 0xff;

        if (version >>> 7 != 1) {
            // This is an indication that the response buffer was incorrectly built
            // Or the buffer offsets are wrong
            throw new SerializationException("The most significant bit should be set according to the format");
        }

        try {
            return ResponseMessageV4.build()
                    .code(HttpResponseStatus.valueOf(context.readValue(buffer, Integer.class, false)))
                    .statusMessage(context.readValue(buffer, String.class, true))
                    .statusAttributes(context.readValue(buffer, Map.class, false))
                    .responseMetaData(context.readValue(buffer, Map.class, false))
                    .result(context.read(buffer))
                    .create();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public void writeValue(final ResponseMessageV4 value, final ByteBuf byteBuf, final GraphBinaryWriter context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        final ResponseResultV4 result = value.getResult();
        final ResponseStatusV4 status = value.getStatus();

        try {
            // Version
            buffer.writeByte(GraphBinaryWriter.VERSION_BYTE);
            // Status code
            context.writeValue(status.getCode().code(), buffer, false);
            // Nullable status message
            context.writeValue(status.getMessage(), buffer, true);
            // Status attributes
            context.writeValue(status.getAttributes(), buffer, false);
            // Result meta
            context.writeValue(result.getMeta(), buffer, false);
            // Fully-qualified value
            context.write(result.getData(), buffer);
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
