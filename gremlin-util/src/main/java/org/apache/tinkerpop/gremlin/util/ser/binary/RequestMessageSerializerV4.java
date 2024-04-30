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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class RequestMessageSerializerV4 {
    private static NettyBufferFactory bufferFactory = new NettyBufferFactory();

    public RequestMessageV4 readValue(final ByteBuf byteBuf, final GraphBinaryReader context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        final int version = buffer.readByte() & 0xff;

        if (version >>> 7 != 1) {
            // This is an indication that the request buffer was incorrectly built
            // Or the buffer offsets are wrong
            throw new SerializationException("The most significant bit should be set according to the format");
        }

        try {
            final Map<String, Object> fields = context.readValue(buffer, Map.class, false);
            final String gremlinType = (String) fields.get("gremlinType");

            final Object gremlin;
            if (gremlinType.equals(Tokens.OPS_EVAL)) {
                gremlin = context.readValue(buffer, String.class, false);
            } else if (gremlinType.equals(Tokens.OPS_BYTECODE)) {
                gremlin = context.readValue(buffer, Bytecode.class, false);
            } else {
                throw new SerializationException("Type " + gremlinType + " not supported for serialization.");
            }

            final RequestMessageV4.Builder builder = RequestMessageV4.build(gremlin);
            if (fields.containsKey(SerTokens.TOKEN_LANGUAGE)) {
                builder.addLanguage(fields.get(SerTokens.TOKEN_LANGUAGE).toString());
            }
            if (fields.containsKey(SerTokens.TOKEN_G)) {
                builder.addG(fields.get(SerTokens.TOKEN_G).toString());
            }
            if (fields.containsKey(SerTokens.TOKEN_BINDINGS)) {
                builder.addBindings((Map<String, Object>) fields.get(SerTokens.TOKEN_BINDINGS));
            }
            if (fields.containsKey(Tokens.TIMEOUT_MS)) {
                builder.addTimeoutMillis((long) fields.get(Tokens.TIMEOUT_MS));
            }
            if (fields.containsKey(Tokens.ARGS_MATERIALIZE_PROPERTIES)) {
                builder.addMaterializeProperties(fields.get(Tokens.ARGS_MATERIALIZE_PROPERTIES).toString());
            }
            if (fields.containsKey(Tokens.ARGS_BATCH_SIZE)) {
                builder.addChunkSize((int) fields.get(Tokens.ARGS_BATCH_SIZE));
            }

            return builder.create();
        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }

    public void writeValue(final RequestMessageV4 value, final ByteBuf byteBuf, final GraphBinaryWriter context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        try {
            // Version
            buffer.writeByte(GraphBinaryWriter.VERSION_BYTE);
            // Fields
            context.writeValue(value.getFields(), buffer, false);
            // Gremlin
            context.writeValue(value.getGremlin(), buffer, false);

        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
