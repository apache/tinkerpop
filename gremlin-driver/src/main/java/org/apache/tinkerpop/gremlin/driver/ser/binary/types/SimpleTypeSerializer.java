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
    private final DataType dataType;

    public DataType getDataType() {
        return dataType;
    }

    public SimpleTypeSerializer(final DataType dataType) {
        this.dataType = dataType;
    }

    @Override
    public T read(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException {
        // No {type_info}, just {value_flag}{value}
        return readValue(buffer, context, true);
    }

    @Override
    public T readValue(final ByteBuf buffer, final GraphBinaryReader context, final boolean nullable) throws SerializationException {
        if (nullable) {
            final byte valueFlag = buffer.readByte();
            if ((valueFlag & 1) == 1) {
                return null;
            }
        }

        return readValue(buffer, context);
    }

    /**
     * Reads a non-nullable value according to the type format.
     * @param buffer A buffer which reader index has been set to the beginning of the {value}.
     * @param context The binary writer.
     * @return
     * @throws SerializationException
     */
    abstract T readValue(final ByteBuf buffer, final GraphBinaryReader context) throws SerializationException;

    @Override
    public ByteBuf write(final T value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException {
        return writeValue(value, allocator, context, true);
    }

    @Override
    public ByteBuf writeValue(final T value, final ByteBufAllocator allocator, final GraphBinaryWriter context, final boolean nullable) throws SerializationException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            return context.getValueFlagNull();
        }

        final ByteBuf valueSequence = writeValue(value, allocator, context);

        if (!nullable) {
            return valueSequence;
        }

        return allocator.compositeBuffer(2).addComponents(true, context.getValueFlagNone(), valueSequence);
    }

    /**
     * Writes a non-nullable value into a buffer using the provided allocator.
     * @param value A non-nullable value.
     * @param allocator The buffer allocator to use.
     * @param context The binary writer.
     * @throws SerializationException
     */
    public abstract ByteBuf writeValue(final T value, final ByteBufAllocator allocator, final GraphBinaryWriter context) throws SerializationException;
}
