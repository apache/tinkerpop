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
package org.apache.tinkerpop.gremlin.driver.ser.binary.types;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class RequestMessageSerializer implements TypeSerializer<RequestMessage> {
    public RequestMessage readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        final byte version = buffer.readByte();
        assert version >>> 7 == 1;

        final UUID id = context.readValue(buffer, UUID.class);
        final String op = context.readValue(buffer, String.class);
        final String processor = context.readValue(buffer, String.class);

        final RequestMessage.Builder builder = RequestMessage.build(op).overrideRequestId(id).processor(processor);

        final Map<String, Object> args = context.readValue(buffer, Map.class);
        args.forEach(builder::addArg);

        return builder.create();
    }

    @Override
    public RequestMessage read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // There is no type code / information for the request message itself.
        throw new SerializationException("RequestMessageSerializer must not invoked with type information");
    }
}
