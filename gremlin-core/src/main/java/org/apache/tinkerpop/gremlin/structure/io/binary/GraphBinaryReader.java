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
package org.apache.tinkerpop.gremlin.structure.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;

/**
 * Reads a value from a buffer using the {@link TypeSerializer} instances configured in the
 * {@link TypeSerializerRegistry}.
 *
 * <p>
 *     This class exposes two different methods to read a value from a buffer: {@link GraphBinaryReader#read(Buffer)}
 *     and {@link GraphBinaryReader#readValue(Buffer, Class, boolean)}:
 *     <ul>
 *         <li>{@code read()} method expects a value in fully-qualified format, composed of
 *         <code>{type_code}{type_info}{value_flag}{value}</code>.</li>
 *         <li>{@code readValue()} method expects a <code>{value_flag}{value}</code> when a value is nullable and
 *         only <code>{value}</code> when a value is not nullable.
 *         </li>
 *     </ul>
 * </p>
 *
 * <p>
 *     The {@link GraphBinaryReader} should be used to read a nested known type from a {@link TypeSerializer}.
 *     For example, if a POINT type is composed by two doubles representing the position in the x and y axes, a
 *     {@link TypeSerializer} for POINT type should use the provided {@link GraphBinaryReader} instance to read those
 *     two double values. As x and y values are expected to be provided as non-nullable doubles, the method
 *     {@code readValue()} should be used: {@code readValue(buffer, Double.class, false)}
 * </p>
 */
public class GraphBinaryReader {
    private final TypeSerializerRegistry registry;

    public GraphBinaryReader() {
        this(TypeSerializerRegistry.INSTANCE);
    }

    public GraphBinaryReader(final TypeSerializerRegistry registry) {
        this.registry = registry;
    }

    /**
     * Reads a value for an specific type.
     *
     * <p>When the value is nullable, the reader expects the <code>{value_flag}{value}</code> to be contained in the
     * buffer.</p>
     *
     * <p>When the value is not nullable, the reader expects only the <code>{value}</code> to be contained in the
     * buffer.</p>
     */
    public <T> T readValue(final Buffer buffer, final Class<T> type, final boolean nullable) throws IOException {
        if (buffer == null) {
            throw new IllegalArgumentException("input cannot be null.");
        } else if (type == null) {
            throw new IllegalArgumentException("type cannot be null.");
        }

        final TypeSerializer<T> serializer = registry.getSerializer(type);
        return serializer.readValue(buffer, this, nullable);
    }

    /**
     * Reads the type code, information and value of a given buffer with fully-qualified format.
     */
    public <T> T read(final Buffer buffer) throws IOException {
        // Fully-qualified format: {type_code}{type_info}{value_flag}{value}
        final DataType type = DataType.get(Byte.toUnsignedInt(buffer.readByte()));

        if (type == DataType.UNSPECIFIED_NULL) {
            // There is no TypeSerializer for unspecified null object
            // Read the value_flag - (folding the buffer.readByte() into the assert does not advance the index so
            // assign to a var first and then do equality on that - learned that the hard way
            final byte check = buffer.readByte();
            assert check == 1;

            // Just return null
            return null;
        }

        TypeSerializer<T> serializer;
        if (type != DataType.CUSTOM) {
            serializer = registry.getSerializer(type);
        } else {
            final String customTypeName = this.readValue(buffer, String.class, false);
            serializer = registry.getSerializerForCustomType(customTypeName);
        }

        return serializer.read(buffer, this);
    }
}
