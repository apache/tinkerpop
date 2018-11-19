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
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializer;

/**
 * Base class for serialization of types that don't contain type specific information only {type_code}, {value_flag}
 * and {value}.
 */
public abstract class SimpleTypeSerializer<T> implements TypeSerializer<T> {
    abstract DataType getDataType();

    @Override
    public T read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
        // No {type_info}, just {value_flag}{value}
        return readValue(buffer, context, true);
    }

    @Override
    public T readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable) throws SerializationException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        return readValue(buffer, context);
    }

    /**
     * Reads a non-nullable value
     */
    abstract T readValue(ByteBuf buffer, GraphBinaryReader context) throws SerializationException;

    @Override
    public ByteBuf write(T value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException {
        final ByteBuf valueBuffer = writeValue(value, allocator, context, true);

        return allocator.compositeBuffer(2)
                .addComponents(
                        true,
                        //TODO: Reuse buffer pooled locally
                        allocator.buffer(1).writeByte(getDataType().getCodeByte()),
                        valueBuffer);
    }

    @Override
    public ByteBuf writeValue(T value, ByteBufAllocator allocator, GraphBinaryWriter context, boolean nullable) throws SerializationException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            return context.getValueFlagNull();
        }

        final ByteBuf valueSequence = writeValueSequence(value, allocator, context);

        if (!nullable) {
            return valueSequence;
        }

        return allocator.compositeBuffer(2).addComponents(true, context.getValueFlagNone(), valueSequence);
    }

    public abstract ByteBuf writeValueSequence(T value, ByteBufAllocator allocator, GraphBinaryWriter context) throws SerializationException;
}
