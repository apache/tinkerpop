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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * The identifier-to-ordinal index of vertices or edges, and the key function and ordering shared by everything that
 * builds one. The index is two parallel segments, {@code id-index-keys.bin} (int64) and {@code id-index-ordinals.bin}
 * (int32), with entries sorted by key and then by ordinal, both compared as signed numbers.
 * <p/>
 * The key of an identifier is:
 * <ul>
 *     <li>its numeric value for the types {@code BYTE}, {@code SHORT}, {@code INT} and {@code LONG};</li>
 *     <li>otherwise a fixed 64-bit hash of the type code and the encoded bytes, see
 *     {@link #keyOf(ValueType, byte[], int, int)}.</li>
 * </ul>
 * The index is <em>exact</em> when the identifier column holds a single integral type: a key then identifies the
 * identifier completely and lookup needs no column access. Otherwise entries with an equal key are checked against the
 * identifier column, comparing type and encoded bytes, so a hash collision or an {@code Integer 1} versus
 * {@code Long 1} is never confused. Identifier equality is thus type plus encoded bytes.
 * <p/>
 * An index is built with {@link IdentifierIndexWriter} and looked up with {@link #lookup(Object)}. Lookups may be
 * made from several threads.
 */
public final class IdentifierIndex {

    private static final VarHandle SHORT = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle LONG = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;

    private final MappedSegment keys;
    private final MappedSegment ordinals;
    private final ValueType exactType;
    private final ColumnReader ids;
    private final long count;

    private IdentifierIndex(final MappedSegment keys, final MappedSegment ordinals, final ValueType exactType,
                            final ColumnReader ids) {
        this.keys = keys;
        this.ordinals = ordinals;
        this.exactType = exactType;
        this.ids = ids;
        this.count = keys.count();
    }

    // ---------------------------------------------------------------- keys and ordering

    /**
     * The single integral type that makes an index for the given identifier column exact, or null if the index is not
     * exact: the column must hold exactly one type and that type must be {@code BYTE}, {@code SHORT}, {@code INT} or
     * {@code LONG}. The manifest's exact-index flag is {@code exactType(info) != null}.
     */
    public static ValueType exactType(final Manifest.ColumnInfo idInfo) {
        if (idInfo.valueTypes().size() != 1) return null;
        final ValueType t = idInfo.valueTypes().get(0);
        return ValueCodec.isIntegral(t) ? t : null;
    }

    /**
     * The index key of an encoded identifier.
     *
     * @param type    the identifier's type
     * @param encoded the buffer holding the identifier's canonical encoding
     */
    public static long keyOf(final ValueType type, final byte[] encoded, final int offset, final int length) {
        switch (type) {
            case BYTE:
                return encoded[offset];
            case SHORT:
                return (short) SHORT.get(encoded, offset);
            case INT:
                return (int) INT.get(encoded, offset);
            case LONG:
                return (long) LONG.get(encoded, offset);
            default:
                return hash(type, encoded, offset, length);
        }
    }

    /**
     * The index key of an identifier object.
     *
     * @throws org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException if the identifier
     *         is null or of an unsupported class
     */
    public static long keyOf(final Object id) {
        final ValueType type = ValueCodec.requireIdentifierType(id, "identifier", null);
        return keyOf(type, id);
    }

    // the type must be the identifier's type
    private static long keyOf(final ValueType type, final Object id) {
        switch (type) {
            case BYTE:
                return (Byte) id;
            case SHORT:
                return (Short) id;
            case INT:
                return (Integer) id;
            case LONG:
                return (Long) id;
            default: {
                final byte[] encoded = ValueCodec.encode(type, id);
                return hash(type, encoded, 0, encoded.length);
            }
        }
    }

    /**
     * The fixed 64-bit hash used as the key of non-integral identifiers: FNV-1a over the type code and the encoded
     * bytes, followed by the MurmurHash3 64-bit finalizer. Every builder and the reader must use this function.
     */
    public static long hash(final ValueType type, final byte[] encoded, final int offset, final int length) {
        long h = FNV_OFFSET;
        h ^= type.code() & 0xff;
        h *= FNV_PRIME;
        for (int i = offset; i < offset + length; i++) {
            h ^= encoded[i] & 0xff;
            h *= FNV_PRIME;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;
        return h;
    }

    /**
     * The index order: by key, then by ordinal, both signed. Builders that sort entries themselves, for example an
     * external sort of a spool, must use this order.
     */
    public static int compare(final long keyA, final int ordinalA, final long keyB, final int ordinalB) {
        final int c = Long.compare(keyA, keyB);
        return c != 0 ? c : Integer.compare(ordinalA, ordinalB);
    }

    /**
     * Sorts the first {@code count} entries of the parallel arrays into index order, stably and in place. Intended for
     * the materializing builder; it needs a temporary copy of both arrays.
     */
    public static void sortEntries(final long[] keys, final int[] ordinals, final int count) {
        Objects.checkFromIndexSize(0, count, keys.length);
        Objects.checkFromIndexSize(0, count, ordinals.length);
        boolean sorted = true;
        for (int i = 1; i < count && sorted; i++) {
            sorted = compare(keys[i - 1], ordinals[i - 1], keys[i], ordinals[i]) <= 0;
        }
        if (sorted) return;
        long[] srcK = keys;
        int[] srcO = ordinals;
        long[] dstK = new long[count];
        int[] dstO = new int[count];
        for (int width = 1; width < count; width <<= 1) {
            for (int lo = 0; lo < count; lo += 2 * width) {
                final int mid = Math.min(lo + width, count);
                final int hi = Math.min(lo + 2 * width, count);
                int a = lo;
                int b = mid;
                int out = lo;
                while (a < mid && b < hi) {
                    if (compare(srcK[a], srcO[a], srcK[b], srcO[b]) <= 0) {
                        dstK[out] = srcK[a];
                        dstO[out++] = srcO[a++];
                    } else {
                        dstK[out] = srcK[b];
                        dstO[out++] = srcO[b++];
                    }
                }
                while (a < mid) {
                    dstK[out] = srcK[a];
                    dstO[out++] = srcO[a++];
                }
                while (b < hi) {
                    dstK[out] = srcK[b];
                    dstO[out++] = srcO[b++];
                }
            }
            final long[] tk = srcK;
            srcK = dstK;
            dstK = tk;
            final int[] to = srcO;
            srcO = dstO;
            dstO = to;
        }
        if (srcK != keys) {
            System.arraycopy(srcK, 0, keys, 0, count);
            System.arraycopy(srcO, 0, ordinals, 0, count);
        }
    }

    // ---------------------------------------------------------------- lookup

    /**
     * Wraps the two mapped index segments for lookup.
     *
     * @param keys      {@code id-index-keys.bin}, width 8
     * @param ordinals  {@code id-index-ordinals.bin}, width 4, with the same count
     * @param exactType the result of {@link #exactType(Manifest.ColumnInfo)}, null for a non-exact index
     * @param ids       the identifier column, required when the index is not exact and otherwise optional
     */
    public static IdentifierIndex of(final MappedSegment keys, final MappedSegment ordinals, final ValueType exactType,
                                     final ColumnReader ids) {
        if (keys.valueWidth() != 8 || ordinals.valueWidth() != 4 || keys.count() != ordinals.count()) {
            throw new IllegalStateException("Identifier index segments " + keys.path() + " and " + ordinals.path()
                    + " do not match: widths " + keys.valueWidth() + " and " + ordinals.valueWidth() + ", counts "
                    + keys.count() + " and " + ordinals.count());
        }
        if (exactType == null && ids == null) {
            throw new IllegalArgumentException("A non-exact identifier index needs the identifier column");
        }
        return new IdentifierIndex(keys, ordinals, exactType, ids);
    }

    /**
     * The number of entries, which is the number of elements.
     */
    public long count() {
        return count;
    }

    public boolean isExact() {
        return exactType != null;
    }

    /**
     * The ordinal of the element with the given identifier, or -1 if there is none. An identifier of a type that cannot
     * be stored, such as null, simply has no match. Identifiers of different types never match each other.
     */
    public int lookup(final Object id) {
        final ValueType type = ValueCodec.typeOf(id);
        if (type == null) return -1;
        if (exactType != null) {
            if (type != exactType) return -1;
            // the type is integral, so the key is the value; the first entry is the only one
            final long key = keyOf(type, id);
            final long first = lowerBound(key);
            return first < count && keys.getLong(first) == key ? ordinals.getInt(first) : -1;
        }
        final byte[] encoded = ValueCodec.encode(type, id);
        final long key = keyOf(type, encoded, 0, encoded.length);
        for (long i = lowerBound(key); i < count && keys.getLong(i) == key; i++) {
            final int ordinal = ordinals.getInt(i);
            if (ids.matches(ordinal, type, encoded)) return ordinal;
        }
        return -1;
    }

    /**
     * The ordinal of the element whose integral identifier has the given numeric value, whatever its width, or -1 if
     * there is none. Only an exact index can answer this, since there every key is the identifier's numeric value and
     * every identifier is integral; no column access is needed.
     *
     * @throws IllegalStateException if the index is not exact
     */
    public int lookupIntegral(final long value) {
        if (exactType == null) throw new IllegalStateException("Integral lookup needs an exact identifier index");
        final long first = lowerBound(value);
        return first < count && keys.getLong(first) == value ? ordinals.getInt(first) : -1;
    }

    // the first entry whose key is >= the given key, or count
    private long lowerBound(final long key) {
        long lo = 0;
        long hi = count;
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (keys.getLong(mid) < key) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
}
