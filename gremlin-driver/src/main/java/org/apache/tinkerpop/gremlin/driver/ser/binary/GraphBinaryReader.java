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
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;

public class GraphBinaryReader {
    private final TypeSerializerRegistry registry;

    public GraphBinaryReader() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryReader(TypeSerializerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Reads a value for an specific type.
     */
    public <T> T readValue(ByteBuf buffer, Class<T> type, boolean nullable) throws SerializationException {
        if(buffer == null) {
            throw new IllegalArgumentException("input cannot be null.");
        } else if(type == null) {
            throw new IllegalArgumentException("type cannot be null.");
        }

        TypeSerializer<T> serializer = registry.getSerializer(type);
        return serializer.readValue(buffer, this, nullable);
    }

    /**
     * Reads the type code, information and value of a given buffer with fully-qualified format.
     */
    public <T> T read(ByteBuf buffer) throws SerializationException {
        // Fully-qualified format: {type_code}{type_info}{value_flag}{value}
        DataType type = DataType.get(buffer.readByte());
        TypeSerializer<T> serializer = registry.getSerializer(type);
        return serializer.read(buffer, this);
    }
}
