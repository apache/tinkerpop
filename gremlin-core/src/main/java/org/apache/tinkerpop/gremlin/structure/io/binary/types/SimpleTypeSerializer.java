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
package org.apache.tinkerpop.gremlin.structure.io.binary.types;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReadLimits;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializer;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

import java.io.IOException;
import java.util.LinkedHashMap;

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
    public T read(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        // No {type_info}, just {value_flag}{value}
        return readValue(buffer, context, true);
    }

    @Override
    public T readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
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
     *
     * @param buffer  A buffer which reader index has been set to the beginning of the {value}.
     * @param context The binary reader.
     * @throws IOException
     * @since 4.0.0
     */
    protected abstract T readValue(final Buffer buffer, final GraphBinaryReader context) throws IOException;

    private static int readRawPrefix(final Buffer buffer) throws IOException {
        final int size = buffer.readInt();
        if (size < 0) {
            throw new IOException(String.format("Invalid GraphBinary length prefix: %d", size));
        }
        return size;
    }

    /**
     * Reads the length of a {@code String}, {@code BigInteger} or binary value. The length is rejected if it is
     * negative, exceeds {@link GraphBinaryReadLimits#getMaxValueLength()} or, where the buffer's size is known,
     * exceeds the bytes remaining.
     */
    protected static int readByteLength(final Buffer buffer, final GraphBinaryReader context) throws IOException {
        final int length = readRawPrefix(buffer);
        final int maxValueLength = context.getLimits().getMaxValueLength();
        if (length > maxValueLength) {
            throw new IOException(String.format(
                    "Invalid GraphBinary length prefix: %d exceeds the maximum allowed value size of %d bytes. " +
                            "The limit can be raised with the maxGraphBinaryValueLength setting.",
                    length, maxValueLength));
        }
        if (buffer.hasKnownRemainingLength() && length > buffer.readableBytes()) {
            throw new IOException(String.format("Invalid GraphBinary length prefix: %d (readable bytes: %d)",
                    length, buffer.readableBytes()));
        }
        return length;
    }

    /**
     * Reads the element count of a growable container such as a list, map or tree. The count is rejected if it is
     * negative or, where the buffer's size is known, exceeds the bytes remaining. It is not otherwise capped because
     * the container grows only as elements are actually read. Size any pre-allocation with
     * {@link #cappedInitialCapacity} rather than the count.
     */
    protected static int readElementCount(final Buffer buffer) throws IOException {
        final int count = readRawPrefix(buffer);
        if (buffer.hasKnownRemainingLength() && count > buffer.readableBytes()) {
            throw new IOException(String.format("Invalid GraphBinary count prefix: %d (readable bytes: %d)",
                    count, buffer.readableBytes()));
        }
        return count;
    }

    /**
     * Limits the pre-allocated capacity of a growable container to
     * {@link GraphBinaryReadLimits#getMaxInitialCapacity()}, as the count comes from untrusted input.
     */
    protected static int cappedInitialCapacity(final int count, final GraphBinaryReader context) {
        return Math.min(count, context.getLimits().getMaxInitialCapacity());
    }

    @Override
    public void write(final T value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
        writeValue(value, buffer, context, true);
    }

    @Override
    public void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context, final boolean nullable) throws IOException {
        if (value == null) {
            if (!nullable) {
                throw new IOException("Unexpected null value when nullable is false");
            }

            context.writeValueFlagNull(buffer);
            return;
        }

        if (nullable) {
            if (value instanceof LinkedHashMap) {
                context.writeValueFlagOrdered(buffer);
            } else if (value instanceof BulkSet) {
                context.writeValueFlagBulk(buffer);
            } else {
                context.writeValueFlagNone(buffer);
            }
        }

        writeValue(value, buffer, context);
    }

    /**
     * Writes a non-nullable value into a buffer using the provided allocator.
     * @param value A non-nullable value.
     * @param buffer The buffer allocator to use.
     * @param context The binary writer.
     * @throws IOException
     */
    protected abstract void writeValue(final T value, final Buffer buffer, final GraphBinaryWriter context) throws IOException;
}
