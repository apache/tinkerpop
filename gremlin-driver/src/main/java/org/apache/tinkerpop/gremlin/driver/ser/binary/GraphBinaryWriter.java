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
import io.netty.buffer.Unpooled;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.TransformSerializer;

public class GraphBinaryWriter {
    private final TypeSerializerRegistry registry;
    private final static byte[] valueFlagNullBytes = new byte[] { 0x01 };
    private final static byte[] valueFlagNoneBytes = new byte[] { 0 };
    private final static byte[] unspecifiedNullBytes = new byte[] { DataType.UNSPECIFIED_NULL.getCodeByte(), 0x01};
    private final static byte[] customTypeCodeBytes = new byte[] { DataType.CUSTOM.getCodeByte() };

    public GraphBinaryWriter() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryWriter(final TypeSerializerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Writes a value without including type information.
     */
    public <T> ByteBuf writeValue(final T value, final ByteBufAllocator allocator, final boolean nullable) throws SerializationException {
        if (value == null) {
            if (!nullable) {
                throw new SerializationException("Unexpected null value when nullable is false");
            }

            return getValueFlagNull();
        }

        final Class<?> objectClass = value.getClass();

        final TypeSerializer<T> serializer = (TypeSerializer<T>) registry.getSerializer(objectClass);
        return serializer.writeValue(value, allocator, this, nullable);
    }

    /**
     * Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
     */
    public <T> ByteBuf write(final T value, final ByteBufAllocator allocator) throws SerializationException {
        if (value == null) {
            // return Object of type "unspecified object null" with the value flag set to null.
            return Unpooled.wrappedBuffer(unspecifiedNullBytes);
        }

        final Class<?> objectClass = value.getClass();
        final TypeSerializer<T> serializer = (TypeSerializer<T>) registry.getSerializer(objectClass);

        if (serializer instanceof CustomTypeSerializer) {
            // It's a custom type
            CustomTypeSerializer customTypeSerializer = (CustomTypeSerializer) serializer;

            // Try to serialize the custom value before allocating a composite buffer
            ByteBuf customTypeValueBuffer = customTypeSerializer.write(value, allocator, this);

            return allocator.compositeBuffer(3)
                    .addComponent(true, Unpooled.wrappedBuffer(customTypeCodeBytes))
                    .addComponent(true, writeValue(customTypeSerializer.getTypeName(), allocator, false))
                    .addComponent(true, customTypeValueBuffer);
        }

        if (serializer instanceof TransformSerializer) {
            // For historical reasons, there are types that need to be transformed into another type
            // before serialization, e.g., Map.Entry
            TransformSerializer<T> transformSerializer = (TransformSerializer<T>) serializer;
            return write(transformSerializer.transform(value), allocator);
        }

        // Try to serialize the value before creating a new composite buffer
        ByteBuf typeInfoAndValueBuffer = serializer.write(value, allocator, this);

        return allocator.compositeBuffer(2).addComponents(true,
                // {type_code}
                Unpooled.wrappedBuffer(serializer.getDataType().getDataTypeBuffer()),
                // {type_info}{value_flag}{value}
                typeInfoAndValueBuffer);
    }

    /**
     * Represents a null value of a specific type, useful when the parent type contains a type parameter that must be
     * specified.
     * <p>Note that for simple types, the provided information will be <code>null</code>.</p>
     */
    public <T> ByteBuf writeFullyQualifiedNull(final Class<T> objectClass, final ByteBufAllocator allocator, final Object information) throws SerializationException {
        TypeSerializer<T> serializer = registry.getSerializer(objectClass);
        return serializer.write(null, allocator, this);
    }

    /**
     * Gets a buffer containing a single byte representing the null value_flag.
     */
    public ByteBuf getValueFlagNull() {
        return Unpooled.wrappedBuffer(valueFlagNullBytes);
    }

    /**
     * Gets a buffer containing a single byte with value 0, representing an unset value_flag.
     */
    public ByteBuf getValueFlagNone() {
        return Unpooled.wrappedBuffer(valueFlagNoneBytes);
    }
}
