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
package org.apache.tinkerpop.gremlin.structure.snapshot.build;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueCodec;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * A reusable buffer in which one spool record is assembled before it is appended to a {@link PropertySpool}. A record
 * is a sequence of int32 and int64 fields and encoded values. An encoded value is its one-byte {@link ValueType} code,
 * then an int32 length when the type is variable-width, then the canonical payload of
 * {@link org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueCodec}. Fixed-width values are encoded without
 * allocating. A null value is only the code of {@link ValueType#NULL}, without a length or a payload. All numbers are
 * little-endian.
 */
final class SpoolRecord {

    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private byte[] buffer = new byte[256];
    private int length;

    void reset() {
        length = 0;
    }

    byte[] bytes() {
        return buffer;
    }

    int length() {
        return length;
    }

    void putInt(final int value) {
        ensure(4);
        INT.set(buffer, length, value);
        length += 4;
    }

    void putLong(final long value) {
        ensure(8);
        LONG.set(buffer, length, value);
        length += 8;
    }

    /**
     * Appends the marker of a null value, which is the code of {@link ValueType#NULL} alone.
     */
    void putNull() {
        ensure(1);
        buffer[length++] = ValueType.NULL.code();
    }

    /**
     * Appends the type code, the length for a variable-width type and the payload of a value.
     *
     * @return the offset in {@link #bytes()} at which the payload starts, which runs to {@link #length()}
     */
    int putValue(final ValueType type, final Object value) {
        ensure(1 + 4);
        buffer[length++] = type.code();
        if (type.isFixedWidth()) {
            final int width = type.width();
            ensure(width);
            final long bits = ValueCodec.fixedBits(type, value);
            final int start = length;
            for (int i = 0; i < width; i++) buffer[start + i] = (byte) (bits >>> (8 * i));
            length += width;
            return start;
        }
        final byte[] encoded = ValueCodec.encode(type, value);
        INT.set(buffer, length, encoded.length);
        length += 4;
        ensure(encoded.length);
        System.arraycopy(encoded, 0, buffer, length, encoded.length);
        final int start = length;
        length += encoded.length;
        return start;
    }

    private void ensure(final int extra) {
        final long needed = (long) length + extra;
        if (needed > buffer.length) {
            if (needed > Integer.MAX_VALUE - 8) throw new IllegalStateException("Spool record too large");
            buffer = Arrays.copyOf(buffer, (int) Math.max(needed, Math.min((long) buffer.length * 2, Integer.MAX_VALUE - 8)));
        }
    }
}
