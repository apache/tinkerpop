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
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseResult;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatus;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;

import java.util.Map;
import java.util.UUID;

public class ResponseMessageSerializer {

    public ResponseMessage readValue(final ByteBuf buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException {
        final int version = buffer.readByte() & 0xff;

        if (version >>> 7 != 1) {
            // This is an indication that the response buffer was incorrectly built
            // Or the buffer offsets are wrong
            throw new SerializationException("The most significant bit should be set according to the format");
        }

        return ResponseMessage.build(context.readValue(buffer, UUID.class, true))
                .code(ResponseStatusCode.getFromValue(context.readValue(buffer, Integer.class, false)))
                .statusMessage(context.readValue(buffer, String.class, true))
                .statusAttributes(context.readValue(buffer, Map.class, false))
                .responseMetaData(context.readValue(buffer, Map.class, false))
                .result(context.read(buffer))
                .create();
    }

    public ByteBuf writeValue(final ResponseMessage value, final ByteBufAllocator allocator, final GraphBinaryWriter context, final boolean nullable) throws SerializationException {
        final ResponseResult result = value.getResult();
        final ResponseStatus status = value.getStatus();

        return allocator.compositeBuffer(8).addComponents(true,
                // Version
                allocator.buffer(1).writeByte(0x81),
                // Nullable request id
                context.writeValue(value.getRequestId(), allocator, true),
                // Status code
                context.writeValue(status.getCode().getValue(), allocator, false),
                // Nullable status message
                context.writeValue(status.getMessage(), allocator, true),
                // Status attributes
                context.writeValue(status.getAttributes(), allocator, false),
                // Result meta
                context.writeValue(result.getMeta(), allocator, false),
                // Fully-qualified value
                context.write(result.getData(), allocator));
    }
}
