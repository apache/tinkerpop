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

    /**
     * Upper bound, in bytes, on a length prefix sizing a fixed byte allocation ({@code byte[]}, {@code ByteBuffer}),
     * enforced even when {@link Buffer#hasKnownRemainingLength()} is {@code false}.
     */
    private static final int MAX_BYTE_LENGTH = 64 * 1024 * 1024;

    /**
     * Upper bound on a count prefix sizing a fixed-size array ({@code Object[]}, {@code Class[]}), enforced even
     * when {@link Buffer#hasKnownRemainingLength()} is {@code false}.
     */
    private static final int MAX_FIXED_ARRAY_COUNT = 1 << 20;

    /**
     * Upper bound on the capacity pre-allocated for a growable container from a wire count.
     */
    private static final int MAX_PREALLOC_CAPACITY = 16384;

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

    /**
     * Reads a raw 4-byte length or count prefix and rejects a negative value.
     */
    private static int readRawPrefix(final Buffer buffer) throws IOException {
        final int size = buffer.readInt();
        if (size < 0) {
            throw new IOException(String.format("Invalid GraphBinary length prefix: %d", size));
        }
        return size;
    }

    /**
     * Reads and validates a byte-length prefix sizing a fixed allocation ({@code byte[]} or {@code ByteBuffer})
     * meant to be filled entirely from the wire, e.g. a {@code String}, {@code BigInteger}, or binary value.
     * Rejects a negative length and any length exceeding {@link #MAX_BYTE_LENGTH}. When
     * {@link Buffer#hasKnownRemainingLength()} is {@code true}, also rejects a length exceeding
     * {@link Buffer#readableBytes()}.
     */
    protected static int readByteLength(final Buffer buffer) throws IOException {
        final int length = readRawPrefix(buffer);
        if (length > MAX_BYTE_LENGTH) {
            throw new IOException(String.format(
                    "Invalid GraphBinary length prefix: %d exceeds the maximum allowed value size of %d bytes",
                    length, MAX_BYTE_LENGTH));
        }
        if (buffer.hasKnownRemainingLength() && length > buffer.readableBytes()) {
            throw new IOException(String.format("Invalid GraphBinary length prefix: %d (readable bytes: %d)",
                    length, buffer.readableBytes()));
        }
        return length;
    }

    /**
     * Reads and validates an element-count prefix for a growable container (e.g. {@code List}, {@code Set},
     * {@code Map}, {@code Tree}). Rejects a negative count. When {@link Buffer#hasKnownRemainingLength()} is
     * {@code true}, also rejects a count exceeding {@link Buffer#readableBytes()}. Callers must size any initial
     * container capacity with {@link #cappedInitialCapacity(int)} rather than the raw count.
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
     * Reads and validates a count prefix that directly sizes a fixed-size array ({@code Object[]},
     * {@code Class[]}), e.g. {@code P} predicate arguments. Rejects a negative count and any count exceeding
     * {@link #MAX_FIXED_ARRAY_COUNT}. When {@link Buffer#hasKnownRemainingLength()} is {@code true}, also rejects
     * a count exceeding {@link Buffer#readableBytes()}.
     */
    protected static int readFixedArrayCount(final Buffer buffer) throws IOException {
        final int count = readRawPrefix(buffer);
        if (count > MAX_FIXED_ARRAY_COUNT) {
            throw new IOException(String.format(
                    "Invalid GraphBinary count prefix: %d exceeds the maximum allowed element count of %d",
                    count, MAX_FIXED_ARRAY_COUNT));
        }
        if (buffer.hasKnownRemainingLength() && count > buffer.readableBytes()) {
            throw new IOException(String.format("Invalid GraphBinary count prefix: %d (readable bytes: %d)",
                    count, buffer.readableBytes()));
        }
        return count;
    }

    /**
     * Caps the initial capacity used to pre-size a growable container ({@code ArrayList}, {@code HashMap}) from a
     * validated wire count. Not for a fixed-size array or a byte buffer that must hold exactly {@code count} entries.
     */
    protected static int cappedInitialCapacity(final int count) {
        return Math.min(count, MAX_PREALLOC_CAPACITY);
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
