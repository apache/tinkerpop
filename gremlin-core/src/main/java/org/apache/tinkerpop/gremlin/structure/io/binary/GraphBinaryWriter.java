/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.binary.types.ProviderDefinedTypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.TransformSerializer;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedType;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeAdapter;
import org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefinedTypeRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * Writes a value to a buffer using the {@link TypeSerializer} instances configured in the
 * {@link TypeSerializerRegistry}.
 *
 * <p>
 *     This class exposes two different methods to write a value to a buffer:
 *     {@link GraphBinaryWriter#write(Object, Buffer)} and
 *     {@link GraphBinaryWriter#writeValue(Object, Buffer, boolean)}:
 *     <ul>
 *         <li>{@code write()} method writes the binary representation of the
 *         <code>{type_code}{type_info}{value_flag}{value}</code> components.</li>
 *         <li>{@code writeValue()} method writes the <code>{value_flag}{value}</code> when a value is nullable and
 *         only <code>{value}</code> when a value is not nullable.
 *         </li>
 *     </ul>
 * </p>
 */
public class GraphBinaryWriter {
    private final TypeSerializerRegistry registry;
    private final ProviderDefinedTypeRegistry pdtRegistry;
    private final static byte VALUE_FLAG_NULL = 1;
    private final static byte VALUE_FLAG_NONE = 0;
    private final static byte VALUE_FLAG_ORDERED = 2;
    private final static byte VALUE_FLAG_BULK = 2;
    public final static byte VERSION_BYTE = (byte)0x84;
    public final static byte BULKED_BYTE = (byte)0x01;
    private final static byte[] unspecifiedNullBytes = new byte[] { DataType.UNSPECIFIED_NULL.getCodeByte(), 0x01};

    public GraphBinaryWriter() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryWriter(final TypeSerializerRegistry registry) {
        this(registry, null);
    }

    public GraphBinaryWriter(final TypeSerializerRegistry registry, final ProviderDefinedTypeRegistry pdtRegistry) {
        this.registry = registry;
        this.pdtRegistry = pdtRegistry;
    }

    /**
     * Writes a value without including type information.
     */
    public <T> void writeValue(final T value, final Buffer buffer, final boolean nullable) throws IOException {
        if (value == null) {
            if (!nullable) {
                throw new IOException("Unexpected null value when nullable is false");
            }

            writeValueFlagNull(buffer);
            return;
        }

        final Class<?> objectClass = value.getClass();

        final TypeSerializer<T> serializer = (TypeSerializer<T>) getSerializerOrAdapterFallback(objectClass);
        if (serializer instanceof ProviderDefinedTypeSerializer && !(value instanceof ProviderDefinedType)) {
            serializer.writeValue((T) dehydrateToPdt(value, objectClass), buffer, this, nullable);
            return;
        }
        serializer.writeValue(value, buffer, this, nullable);
    }

    /**
     * Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
     */
    public <T> void write(final T value, final Buffer buffer) throws IOException {
        if (value == null) {
            // return Object of type "unspecified object null" with the value flag set to null.
            buffer.writeBytes(unspecifiedNullBytes);
            return;
        }

        final Class<?> objectClass = value.getClass();
        final TypeSerializer<T> serializer = (TypeSerializer<T>) getSerializerOrAdapterFallback(objectClass);

        if (serializer instanceof ProviderDefinedTypeSerializer && !(value instanceof ProviderDefinedType)) {
            // Convert to ProviderDefinedType (via annotation or adapter), then re-enter write().
            // On re-entry, ProviderDefinedType.class is directly registered in the registry,
            // and the instanceof guard prevents double-wrapping.
            write((T) dehydrateToPdt(value, objectClass), buffer);
            return;
        }

        if (serializer instanceof TransformSerializer) {
            // For historical reasons, there are types that need to be transformed into another type
            // before serialization, e.g., Map.Entry
            final TransformSerializer<T> transformSerializer = (TransformSerializer<T>) serializer;
            write(transformSerializer.transform(value), buffer);
            return;
        }

        // Try to serialize the value before creating a new composite buffer
        buffer.writeBytes(serializer.getDataType().getDataTypeBuffer());
        serializer.write(value, buffer, this);
    }

    /**
     * Represents a null value of a specific type, useful when the parent type contains a type parameter that must be
     * specified.
     * <p>Note that for simple types, the provided information will be <code>null</code>.</p>
     */
    public <T> void writeFullyQualifiedNull(final Class<T> objectClass, Buffer buffer, final Object information) throws IOException {
        final TypeSerializer<T> serializer = registry.getSerializer(objectClass);
        serializer.write(null, buffer, this);
    }

    /**
     * Writes a single byte representing the null value_flag.
     */
    public void writeValueFlagNull(Buffer buffer) {
        buffer.writeByte(VALUE_FLAG_NULL);
    }

    /**
     * Writes a single byte with value 0, representing an unset value_flag.
     */
    public void writeValueFlagNone(Buffer buffer) {
        buffer.writeByte(VALUE_FLAG_NONE);
    }

    /**
     * Writes a single byte with value 2, representing an ordered value_flag.
     */
    public void writeValueFlagOrdered(Buffer buffer) {
        buffer.writeByte(VALUE_FLAG_ORDERED);
    }

    /**
     * Writes a single byte with value 2, representing an ordered value_flag.
     */
    public void writeValueFlagBulk(Buffer buffer) {
        buffer.writeByte(VALUE_FLAG_BULK);
    }

    /**
     * Attempts to get a serializer for the given class. If no serializer is found and the pdtRegistry
     * has an adapter for the class, returns the CompositePDT serializer.
     */
    @SuppressWarnings("unchecked")
    private <DT> TypeSerializer<DT> getSerializerOrAdapterFallback(final Class<?> type) throws IOException {
        try {
            return (TypeSerializer<DT>) registry.getSerializer(type);
        } catch (final IOException e) {
            if (pdtRegistry != null && pdtRegistry.getAdapterByClass(type).isPresent()) {
                return (TypeSerializer<DT>) registry.getSerializer(DataType.COMPOSITE_PDT);
            }
            throw e;
        }
    }

    /**
     * Dehydrates a value to a {@link ProviderDefinedType} using annotation reflection or an adapter from the
     * pdtRegistry.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private ProviderDefinedType dehydrateToPdt(final Object value, final Class<?> objectClass) {
        // Prefer annotation-based conversion
        if (objectClass.isAnnotationPresent(org.apache.tinkerpop.gremlin.structure.io.pdt.ProviderDefined.class)) {
            return ProviderDefinedType.from(value);
        }
        // Fall back to adapter-based conversion
        if (pdtRegistry != null) {
            final Optional<ProviderDefinedTypeAdapter<?>> opt = pdtRegistry.getAdapterByClass(objectClass);
            if (opt.isPresent()) {
                final ProviderDefinedTypeAdapter adapter = opt.get();
                final Map<String, Object> fields = adapter.toFields(value);
                return new ProviderDefinedType(adapter.typeName(), fields);
            }
        }
        // Should not reach here since getSerializerOrAdapterFallback already validated
        return ProviderDefinedType.from(value);
    }

}
