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
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.util.ser.NettyBuffer;
import org.apache.tinkerpop.gremlin.util.ser.NettyBufferFactory;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseResult;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

public class ResponseMessageSerializer {
    public static final int RESPONSE_START_SIZE = -1;
    private static final NettyBufferFactory bufferFactory = new NettyBufferFactory();
    ByteBuf retainedBuf = Unpooled.buffer(0);
    int retainedSize = RESPONSE_START_SIZE;

    public ResponseMessage readValue(final ByteBuf byteBuf, final GraphBinaryReader context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(retainedBuf.readableBytes() > 0 ? retainedBuf : byteBuf);

        if (retainedBuf.readableBytes() == 0) {
            retainedBuf.release();
            retainedBuf = Unpooled.buffer(0);
        }

        if (retainedSize == RESPONSE_START_SIZE && byteBuf.readableBytes() >= 5) {
            final int version = buffer.readByte() & 0xff;

            if (version >>> 7 != 1) {
                // This is an indication that the response buffer was incorrectly built
                // Or the buffer offsets are wrong
                throw new SerializationException("The most significant bit should be set according to the format");
            }

            retainedSize = buffer.readInt() - 5;

            if ((retainedBuf.readableBytes() + buffer.readableBytes()) < retainedSize) {
                retainedBuf.writeBytes(byteBuf);

                return null;
            } else {

                if (byteBuf.readableBytes() > 0) {
                    retainedBuf.writeBytes(byteBuf);
                }

                try {
                    final Buffer messageBuffer = bufferFactory.create(retainedBuf.readSlice(retainedSize));
                    ResponseMessage respMsg = ResponseMessage.build(context.readValue(messageBuffer, UUID.class, true))
                            .code(ResponseStatusCode.getFromValue(context.readValue(messageBuffer, Integer.class, false)))
                            .statusMessage(context.readValue(messageBuffer, String.class, true))
                            .statusAttributes(context.readValue(messageBuffer, Map.class, false))
                            .responseMetaData(context.readValue(messageBuffer, Map.class, false))
                            .result(context.read(messageBuffer))
                            .create();

                    retainedSize = RESPONSE_START_SIZE;

                    return respMsg;
                } catch (Exception ex) {
                    throw new SerializationException(ex);
                }
            }
        } else {
            retainedBuf.writeBytes(byteBuf);

            if (((retainedBuf.readableBytes()) < retainedSize) || retainedSize == RESPONSE_START_SIZE) {
                return null;
            } else {
                try {
                    final Buffer messageBuffer = bufferFactory.create(retainedBuf.readSlice(retainedSize));
                    ResponseMessage respMsg = ResponseMessage.build(context.readValue(messageBuffer, UUID.class, true))
                            .code(ResponseStatusCode.getFromValue(context.readValue(messageBuffer, Integer.class, false)))
                            .statusMessage(context.readValue(messageBuffer, String.class, true))
                            .statusAttributes(context.readValue(messageBuffer, Map.class, false))
                            .responseMetaData(context.readValue(messageBuffer, Map.class, false))
                            .result(context.read(messageBuffer))
                            .create();

                    retainedSize = RESPONSE_START_SIZE;

                    return respMsg;
                } catch (Exception ex) {
                    throw new SerializationException(ex);
                }
            }
        }
    }

    public void writeValue(final ResponseMessage value, final ByteBuf byteBuf, final GraphBinaryWriter context) throws SerializationException {
        // Wrap netty's buffer
        final Buffer buffer = bufferFactory.create(byteBuf);

        final ResponseResult result = value.getResult();
        final ResponseStatus status = value.getStatus();

        try {
            // Version
            buffer.writeByte(GraphBinaryWriter.VERSION_BYTE);
            // Make placeholder for size
            buffer.writeInt(0);
            // Nullable request id
            context.writeValue(value.getRequestId(), buffer, true);
            // Status code
            context.writeValue(status.getCode().getValue(), buffer, false);
            // Nullable status message
            context.writeValue(status.getMessage(), buffer, true);
            // Status attributes
            context.writeValue(status.getAttributes(), buffer, false);
            // Result meta
            context.writeValue(result.getMeta(), buffer, false);
            // Fully-qualified value
            context.write(result.getData(), buffer);
            // overwrite version
            buffer.markWriterIndex();
            final int readable = buffer.readableBytes(); // set this because changing writer index affects its value
            buffer.writerIndex(1);
            buffer.writeInt(readable);
            buffer.resetWriterIndex();

        } catch (IOException ex) {
            throw new SerializationException(ex);
        }
    }
}
