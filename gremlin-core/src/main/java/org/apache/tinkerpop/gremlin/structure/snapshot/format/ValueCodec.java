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
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;

import java.io.ByteArrayOutputStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Encodes and decodes the value types a snapshot can store, following the canonical encodings of
 * {@link ValueType}. All multi-byte numbers are little-endian, like the segments. Equality of encoded bytes together
 * with the {@link ValueType} defines value (and so identifier) equality inside a snapshot.
 * <p/>
 * The supported Java classes are exactly {@link Boolean}, {@link Byte}, {@link Short}, {@link Character},
 * {@link Integer}, {@link Float}, {@link Long}, {@link Double}, {@link String}, {@link UUID}, {@link BigInteger},
 * {@link BigDecimal}, {@link Duration} and {@link OffsetDateTime} (the scalar types), {@code byte[]}, and any
 * {@link List}, {@link Set} or {@link Map} whose entries are themselves supported or null. Subclasses of the scalar
 * classes and every other type, including a top-level {@code null}, are rejected with
 * {@link UnsupportedSnapshotDataException}. A string that is not well-formed UTF-16
 * (an unpaired surrogate) is rejected as well, because it cannot be represented in UTF-8 without loss.
 * <p/>
 * Fixed-width types can also be handled as a {@code long} of "bits", with no allocation: the value for integral types,
 * the raw IEEE bits for {@code FLOAT} and {@code DOUBLE}, 0 or 1 for {@code BOOLEAN}, and the unsigned value for
 * {@code CHAR}. Only the low {@link ValueType#width()} bytes of the bits are significant. The encoded form of a
 * fixed-width value is those bytes in little-endian order.
 * <p/>
 * A collection is an unsigned LEB128 varint entry count followed by the entries. An entry is its {@link ValueType}
 * code byte, an unsigned LEB128 varint payload length and the payload, except that a {@code null} entry is only the
 * {@link ValueType#NULL} code byte. A {@code LIST} keeps list order. {@code SET} entries and {@code MAP} entries
 * (ordered by key, each key entry followed by its value entry) are written in canonical order: by type code and then by
 * payload bytes, both compared as unsigned bytes. Decoding gives an {@link ArrayList}, a {@link LinkedHashSet} in
 * stored order and a {@link LinkedHashMap}. Collections may be nested. Identifiers must be scalar, see
 * {@link #requireIdentifierType}.
 */
public final class ValueCodec {

    private static final VarHandle SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final ValueType[] BY_CODE = new ValueType[256];

    static {
        for (final ValueType t : ValueType.values()) BY_CODE[t.code() & 0xff] = t;
    }

    private ValueCodec() {
    }

    /**
     * Resolves a type from its on-disk code without throwing, in constant time.
     *
     * @return the type, or null if the code is 0 or unknown
     */
    public static ValueType typeOfCode(final byte code) {
        return BY_CODE[code & 0xff];
    }

    /**
     * The type of a value, or null if the value is null or of an unsupported class. Does not allocate.
     */
    public static ValueType typeOf(final Object value) {
        if (value == null) return null;
        final Class<?> c = value.getClass();
        if (c == String.class) return ValueType.STRING;
        if (c == Long.class) return ValueType.LONG;
        if (c == Integer.class) return ValueType.INT;
        if (c == Double.class) return ValueType.DOUBLE;
        if (c == Boolean.class) return ValueType.BOOLEAN;
        if (c == Float.class) return ValueType.FLOAT;
        if (c == Short.class) return ValueType.SHORT;
        if (c == Byte.class) return ValueType.BYTE;
        if (c == Character.class) return ValueType.CHAR;
        if (c == UUID.class) return ValueType.UUID;
        if (c == BigInteger.class) return ValueType.BIGINT;
        if (c == BigDecimal.class) return ValueType.BIGDECIMAL;
        if (c == Duration.class) return ValueType.DURATION;
        if (c == OffsetDateTime.class) return ValueType.DATETIME;
        if (c == byte[].class) return ValueType.BINARY;
        if (value instanceof List) return ValueType.LIST;
        if (value instanceof Set) return ValueType.SET;
        if (value instanceof Map) return ValueType.MAP;
        return null;
    }

    /**
     * The type of a value, rejecting null and unsupported classes loudly.
     *
     * @param kind what the value is, for the error message, for example {@code "vertex property"} or
     *             {@code "edge identifier"}
     * @param key  the property key for the error message, or null when the value is not a property
     * @throws UnsupportedSnapshotDataException if the value is null or its class is not supported
     */
    public static ValueType requireType(final Object value, final String kind, final String key) {
        final ValueType type = typeOf(value);
        if (type == null) {
            final String where = key == null ? kind : kind + " '" + key + "'";
            if (value == null) throw new UnsupportedSnapshotDataException("Null value for " + where + " is not supported");
            throw new UnsupportedSnapshotDataException("Value of unsupported type " + value.getClass().getName()
                    + " for " + where);
        }
        return type;
    }

    /**
     * The type of an identifier, rejecting null, unsupported classes and the non-scalar {@code BINARY}, {@code LIST},
     * {@code SET} and {@code MAP} types loudly.
     *
     * @param kind what the identifier is, for the error message, for example {@code "vertex identifier"}
     * @param key  the property key for the error message, or null when the identifier is not a property's
     * @throws UnsupportedSnapshotDataException if the identifier is null, unsupported or not scalar
     */
    public static ValueType requireIdentifierType(final Object id, final String kind, final String key) {
        final ValueType type = requireType(id, kind, key);
        if (!type.isScalar()) {
            final String where = key == null ? kind : kind + " '" + key + "'";
            throw new UnsupportedSnapshotDataException("Non-scalar " + type + " value for " + where
                    + " is not supported as an identifier");
        }
        return type;
    }

    /**
     * Whether a fixed-width or variable-width type is one of the integral types whose numeric value is used directly as
     * an identifier index key: {@code BYTE}, {@code SHORT}, {@code INT} or {@code LONG}.
     */
    public static boolean isIntegral(final ValueType type) {
        return type == ValueType.BYTE || type == ValueType.SHORT || type == ValueType.INT || type == ValueType.LONG;
    }

    // ---------------------------------------------------------------- fixed-width values as bits

    /**
     * The bits of a fixed-width value, see the class description. The value must be of class matching {@code type}.
     *
     * @throws IllegalArgumentException if the type is not fixed-width
     */
    public static long fixedBits(final ValueType type, final Object value) {
        switch (type) {
            case BOOLEAN:
                return ((Boolean) value) ? 1L : 0L;
            case BYTE:
                return (Byte) value;
            case SHORT:
                return (Short) value;
            case CHAR:
                return (Character) value;
            case INT:
                return (Integer) value;
            case FLOAT:
                return Float.floatToRawIntBits((Float) value);
            case LONG:
                return (Long) value;
            case DOUBLE:
                return Double.doubleToRawLongBits((Double) value);
            default:
                throw new IllegalArgumentException("Not a fixed-width type: " + type);
        }
    }

    /**
     * The value of a fixed-width type from its bits.
     *
     * @throws IllegalArgumentException if the type is not fixed-width
     */
    public static Object fixedValue(final ValueType type, final long bits) {
        switch (type) {
            case BOOLEAN:
                return (bits & 0xff) != 0;
            case BYTE:
                return (byte) bits;
            case SHORT:
                return (short) bits;
            case CHAR:
                return (char) bits;
            case INT:
                return (int) bits;
            case FLOAT:
                return Float.intBitsToFloat((int) bits);
            case LONG:
                return bits;
            case DOUBLE:
                return Double.longBitsToDouble(bits);
            default:
                throw new IllegalArgumentException("Not a fixed-width type: " + type);
        }
    }

    /**
     * The bits of a fixed-width value from its encoded bytes.
     *
     * @throws IllegalArgumentException if the type is not fixed-width or {@code length} is not its width
     */
    public static long fixedBitsFromBytes(final ValueType type, final byte[] buf, final int offset, final int length) {
        if (!type.isFixedWidth() || length != type.width()) {
            throw new IllegalArgumentException("Cannot read " + length + " bytes as " + type);
        }
        switch (type.width()) {
            case 1:
                return buf[offset];
            case 2:
                return (short) SHORT.get(buf, offset);
            case 4:
                return (int) INT.get(buf, offset);
            default:
                return (long) LONG.get(buf, offset);
        }
    }

    /**
     * The encoded bytes of a fixed-width value given its bits.
     *
     * @throws IllegalArgumentException if the type is not fixed-width
     */
    public static byte[] fixedBitsToBytes(final ValueType type, final long bits) {
        if (!type.isFixedWidth()) throw new IllegalArgumentException("Not a fixed-width type: " + type);
        final byte[] out = new byte[type.width()];
        switch (type.width()) {
            case 1:
                out[0] = (byte) bits;
                break;
            case 2:
                SHORT.set(out, 0, (short) bits);
                break;
            case 4:
                INT.set(out, 0, (int) bits);
                break;
            default:
                LONG.set(out, 0, bits);
                break;
        }
        return out;
    }

    // ---------------------------------------------------------------- general encode / decode

    /**
     * Encodes a value of the given type into its canonical bytes. For fixed-width types this is
     * {@link #fixedBitsToBytes}. The {@code type} must be the value's {@link #typeOf type}.
     *
     * @throws UnsupportedSnapshotDataException if a string is not well-formed UTF-16
     */
    public static byte[] encode(final ValueType type, final Object value) {
        switch (type) {
            case STRING: {
                final String s = (String) value;
                checkWellFormed(s);
                return s.getBytes(StandardCharsets.UTF_8);
            }
            case UUID: {
                final UUID u = (UUID) value;
                final byte[] out = new byte[16];
                LONG.set(out, 0, u.getMostSignificantBits());
                LONG.set(out, 8, u.getLeastSignificantBits());
                return out;
            }
            case BIGINT:
                return ((BigInteger) value).toByteArray();
            case BIGDECIMAL: {
                final BigDecimal d = (BigDecimal) value;
                final byte[] unscaled = d.unscaledValue().toByteArray();
                final byte[] out = new byte[4 + unscaled.length];
                INT.set(out, 0, d.scale());
                System.arraycopy(unscaled, 0, out, 4, unscaled.length);
                return out;
            }
            case DURATION: {
                final Duration d = (Duration) value;
                final byte[] out = new byte[12];
                LONG.set(out, 0, d.getSeconds());
                INT.set(out, 8, d.getNano());
                return out;
            }
            case DATETIME: {
                final OffsetDateTime t = (OffsetDateTime) value;
                final byte[] out = new byte[16];
                LONG.set(out, 0, t.toEpochSecond());
                INT.set(out, 8, t.getNano());
                INT.set(out, 12, t.getOffset().getTotalSeconds());
                return out;
            }
            case BINARY:
                return ((byte[]) value).clone();
            case LIST: {
                final List<?> list = (List<?>) value;
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                putVarint(out, list.size());
                for (final Object e : list) Entry.of(e).write(out);
                return out.toByteArray();
            }
            case SET: {
                final Set<?> set = (Set<?>) value;
                final List<Entry> entries = new ArrayList<>(set.size());
                for (final Object e : set) entries.add(Entry.of(e));
                entries.sort(null);
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                putVarint(out, entries.size());
                for (final Entry e : entries) e.write(out);
                return out.toByteArray();
            }
            case MAP: {
                final Map<?, ?> map = (Map<?, ?>) value;
                final List<Entry[]> pairs = new ArrayList<>(map.size());
                for (final Map.Entry<?, ?> e : map.entrySet()) {
                    pairs.add(new Entry[]{Entry.of(e.getKey()), Entry.of(e.getValue())});
                }
                pairs.sort((a, b) -> a[0].compareTo(b[0]));
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                putVarint(out, pairs.size());
                for (final Entry[] pair : pairs) {
                    pair[0].write(out);
                    pair[1].write(out);
                }
                return out.toByteArray();
            }
            case NULL:
                throw new IllegalArgumentException("NULL has no encoding outside a collection");
            default:
                return fixedBitsToBytes(type, fixedBits(type, value));
        }
    }

    /**
     * Determines the type of a value and encodes it.
     *
     * @throws UnsupportedSnapshotDataException if the value is null, of an unsupported class, or an ill-formed string
     */
    public static byte[] encode(final Object value) {
        return encode(requireType(value, "value", null), value);
    }

    /**
     * Decodes {@code length} bytes at {@code offset} as a value of the given type. This is the inverse of
     * {@link #encode(ValueType, Object)}.
     *
     * @throws IllegalArgumentException if the length is impossible for the type
     */
    public static Object decode(final ValueType type, final byte[] buf, final int offset, final int length) {
        switch (type) {
            case STRING:
                return new String(buf, offset, length, StandardCharsets.UTF_8);
            case UUID:
                requireLength(type, length, 16);
                return new UUID((long) LONG.get(buf, offset), (long) LONG.get(buf, offset + 8));
            case BIGINT:
                if (length < 1) throw new IllegalArgumentException("Cannot decode an empty BIGINT");
                return new BigInteger(buf, offset, length);
            case BIGDECIMAL:
                if (length < 5) throw new IllegalArgumentException("Cannot decode BIGDECIMAL of " + length + " bytes");
                return new BigDecimal(new BigInteger(buf, offset + 4, length - 4), (int) INT.get(buf, offset));
            case DURATION:
                requireLength(type, length, 12);
                return Duration.ofSeconds((long) LONG.get(buf, offset), (int) INT.get(buf, offset + 8));
            case DATETIME:
                requireLength(type, length, 16);
                return OffsetDateTime.ofInstant(Instant.ofEpochSecond((long) LONG.get(buf, offset),
                        (int) INT.get(buf, offset + 8)), ZoneOffset.ofTotalSeconds((int) INT.get(buf, offset + 12)));
            case BINARY:
                return Arrays.copyOfRange(buf, offset, offset + length);
            case LIST: {
                final int[] pos = {offset};
                final int end = offset + length;
                final long count = readVarint(buf, pos, end);
                final List<Object> out = new ArrayList<>((int) Math.min(count, 1024));
                for (long i = 0; i < count; i++) out.add(readEntry(buf, pos, end));
                requireConsumed(type, pos[0], end);
                return out;
            }
            case SET: {
                final int[] pos = {offset};
                final int end = offset + length;
                final long count = readVarint(buf, pos, end);
                final Set<Object> out = new LinkedHashSet<>();
                for (long i = 0; i < count; i++) out.add(readEntry(buf, pos, end));
                requireConsumed(type, pos[0], end);
                return out;
            }
            case MAP: {
                final int[] pos = {offset};
                final int end = offset + length;
                final long count = readVarint(buf, pos, end);
                final Map<Object, Object> out = new LinkedHashMap<>();
                for (long i = 0; i < count; i++) {
                    final Object k = readEntry(buf, pos, end);
                    out.put(k, readEntry(buf, pos, end));
                }
                requireConsumed(type, pos[0], end);
                return out;
            }
            case NULL:
                throw new IllegalArgumentException("NULL has no encoding outside a collection");
            default:
                return fixedValue(type, fixedBitsFromBytes(type, buf, offset, length));
        }
    }

    /**
     * Decodes a whole array, see {@link #decode(ValueType, byte[], int, int)}.
     */
    public static Object decode(final ValueType type, final byte[] encoded) {
        return decode(type, encoded, 0, encoded.length);
    }

    // ---------------------------------------------------------------- collections

    /**
     * Appends an unsigned LEB128 varint.
     */
    private static void putVarint(final ByteArrayOutputStream out, final long value) {
        long v = value;
        while ((v & ~0x7fL) != 0) {
            out.write((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }

    private static long readVarint(final byte[] buf, final int[] pos, final int end) {
        long result = 0;
        int shift = 0;
        while (true) {
            if (pos[0] >= end || shift > 63) throw new IllegalArgumentException("Malformed varint in collection");
            final int b = buf[pos[0]++];
            result |= (long) (b & 0x7f) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
        }
    }

    // reads the entry at pos[0] and advances pos
    private static Object readEntry(final byte[] buf, final int[] pos, final int end) {
        if (pos[0] >= end) throw new IllegalArgumentException("Truncated collection entry");
        final ValueType type = typeOfCode(buf[pos[0]++]);
        if (type == null) throw new IllegalArgumentException("Invalid type code in collection entry");
        if (type == ValueType.NULL) return null;
        final long length = readVarint(buf, pos, end);
        if (length > end - pos[0]) throw new IllegalArgumentException("Truncated collection entry payload");
        final Object value = decode(type, buf, pos[0], (int) length);
        pos[0] += (int) length;
        return value;
    }

    private static void requireConsumed(final ValueType type, final int position, final int end) {
        if (position != end) {
            throw new IllegalArgumentException("Cannot decode " + type + ": " + (end - position) + " trailing bytes");
        }
    }

    // one collection entry in encoded form, ordered canonically: by type code and then by payload, unsigned
    private static final class Entry implements Comparable<Entry> {
        private final ValueType type;
        private final byte[] payload;

        private Entry(final ValueType type, final byte[] payload) {
            this.type = type;
            this.payload = payload;
        }

        static Entry of(final Object value) {
            if (value == null) return new Entry(ValueType.NULL, new byte[0]);
            final ValueType type = requireType(value, "collection entry", null);
            return new Entry(type, encode(type, value));
        }

        void write(final ByteArrayOutputStream out) {
            out.write(type.code());
            if (type == ValueType.NULL) return;
            putVarint(out, payload.length);
            out.write(payload, 0, payload.length);
        }

        @Override
        public int compareTo(final Entry other) {
            final int c = Integer.compare(type.code() & 0xff, other.type.code() & 0xff);
            return c != 0 ? c : Arrays.compareUnsigned(payload, other.payload);
        }
    }

    private static void requireLength(final ValueType type, final int actual, final int expected) {
        if (actual != expected) {
            throw new IllegalArgumentException("Cannot decode " + type + " from " + actual + " bytes, expected " + expected);
        }
    }

    // rejects unpaired surrogates, which String.getBytes would silently replace with '?'
    private static void checkWellFormed(final String s) {
        final int n = s.length();
        for (int i = 0; i < n; i++) {
            final char c = s.charAt(i);
            if (Character.isSurrogate(c)) {
                if (Character.isHighSurrogate(c) && i + 1 < n && Character.isLowSurrogate(s.charAt(i + 1))) {
                    i++;
                } else {
                    throw new UnsupportedSnapshotDataException("String contains an unpaired surrogate at index " + i
                            + " and cannot be encoded as UTF-8");
                }
            }
        }
    }
}
