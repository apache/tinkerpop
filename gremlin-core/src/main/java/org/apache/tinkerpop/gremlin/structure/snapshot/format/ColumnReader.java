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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * Random access to one column directory written by {@link ColumnWriter}, through read-only memory-mapped segments.
 * {@link #get(long)} decodes the value of an element and {@link #type(long)} returns its {@link ValueType}; both return
 * null for an element that has no entry and for an element whose entry is a null value, so callers that must tell the
 * two apart use {@link #isPresent(long)} and {@link #isNull(long)}.
 * <p/>
 * The entry-index methods ({@link #getAt}, {@link #typeAt}, {@link #isNullAt}) address the column's entries directly,
 * which is how the multi vertex-property layout reads a key's values by vertex-property ordinal: there the column is
 * dense and complete, so the entry index is the ordinal. {@link #entryCount()} is the number of entries. Reading an
 * absent position of a dense column by entry index is not meaningful.
 * <p/>
 * A dense column answers in constant time. A sparse column binary-searches {@code ordinals.bin}. Instances may be
 * used from several threads.
 * <p/>
 * The segments are obtained through a {@code Function<String, MappedSegment>} from a path relative to the bundle root,
 * so the snapshot reader can validate headers and share its mappings; see {@link #readOnlyOpener(Path)} for the
 * plain implementation. The segment headers are checked against what the {@link Manifest.ColumnInfo} implies, and a
 * mismatch fails loudly with {@link IllegalStateException}.
 */
public final class ColumnReader implements AutoCloseable {

    private final String relativeDir;
    private final long elementCount;
    private final Manifest.ColumnInfo info;
    private final boolean dense;
    private final boolean fixed;
    private final boolean mixed;
    private final ValueType singleType;
    private final MappedSegment presence;
    private final MappedSegment ordinals;
    private final MappedSegment values;
    private final MappedSegment offsets;
    private final MappedSegment data;
    private final MappedSegment types;
    private final MappedSegment nulls;
    private final List<MappedSegment> segments = new ArrayList<>();

    private ColumnReader(final Function<String, MappedSegment> opener, final String relativeDir,
                         final long elementCount, final Manifest.ColumnInfo info, final ColumnReader parent) {
        this.relativeDir = relativeDir;
        this.elementCount = elementCount;
        this.info = info;
        this.dense = ColumnWriter.isDense(info.encoding());
        this.fixed = ColumnWriter.isFixed(info.encoding());
        this.mixed = info.valueTypes().size() > 1;
        this.singleType = info.valueTypes().size() == 1 ? info.valueTypes().get(0) : null;
        if (fixed && (singleType == null || !singleType.isFixedWidth())) {
            throw corrupt("encoding " + info.encoding() + " requires exactly one fixed-width type, found "
                    + info.valueTypes());
        }
        final long present = info.presentCount();
        if (present < 0 || present > elementCount) {
            throw corrupt("present count " + present + " exceeds the element count " + elementCount);
        }
        if (dense != ColumnStats.isDense(elementCount, present)) {
            throw corrupt("encoding " + info.encoding() + " does not match " + present + " of " + elementCount
                    + " elements");
        }
        if (parent == null) {
            if (dense && present < elementCount) {
                presence = map(opener, SegmentPaths.PRESENCE, 8, (elementCount + 63) >>> 6);
            } else {
                presence = null;
            }
            ordinals = dense ? null : map(opener, SegmentPaths.ORDINALS, 4, present);
        } else {
            if (parent.elementCount != elementCount || parent.info.presentCount() != present
                    || parent.dense != dense) {
                throw corrupt("does not match the shape of its parent column " + parent.relativeDir);
            }
            presence = parent.presence;
            ordinals = parent.ordinals;
        }
        final long entries = dense ? elementCount : present;
        if (fixed) {
            values = map(opener, SegmentPaths.VALUES, singleType.width(), entries);
            offsets = null;
            data = null;
        } else {
            values = null;
            offsets = map(opener, SegmentPaths.OFFSETS, 8, entries + 1);
            data = map(opener, SegmentPaths.DATA, 1, offsets.getLong(entries));
        }
        types = mixed ? map(opener, SegmentPaths.TYPES, 1, entries) : null;
        if (info.nullCount() < 0 || info.nullCount() > present) {
            throw corrupt("null count " + info.nullCount() + " exceeds the present count " + present);
        }
        nulls = info.nullCount() > 0 ? map(opener, SegmentPaths.NULLS, 8, (entries + 63) >>> 6) : null;
    }

    private MappedSegment map(final Function<String, MappedSegment> opener, final String fileName, final int width,
                              final long count) {
        final String path = SegmentPaths.file(relativeDir, fileName);
        final MappedSegment seg = opener.apply(path);
        if (seg.valueWidth() != width || seg.count() != count) {
            throw corrupt(path + " has width " + seg.valueWidth() + " and count " + seg.count() + ", expected width "
                    + width + " and count " + count);
        }
        segments.add(seg);
        return seg;
    }

    private IllegalStateException corrupt(final String message) {
        return new IllegalStateException("Column " + relativeDir + ": " + message);
    }

    /**
     * An opener that maps {@code root/<relativePath>} read-only with the default region size. Each call maps the file
     * again, so a caller that wants to share or track mappings should supply its own function.
     */
    public static Function<String, MappedSegment> readOnlyOpener(final Path root) {
        return readOnlyOpener(root, MappedSegment.DEFAULT_REGION_BYTES);
    }

    /**
     * As {@link #readOnlyOpener(Path)} with an explicit region size.
     */
    public static Function<String, MappedSegment> readOnlyOpener(final Path root, final long regionBytes) {
        return relativePath -> MappedSegment.open(root.resolve(relativePath), MappedSegment.Mode.READ_ONLY,
                regionBytes);
    }

    /**
     * Opens a column.
     *
     * @param opener       maps a bundle-relative segment path, for example {@link #readOnlyOpener(Path)}
     * @param relativeDir  the column directory relative to the bundle root
     * @param elementCount the number of elements of the column's kind: all vertices, or all edges
     * @param info         the column's manifest entry
     * @throws IllegalStateException if a segment does not match what {@code info} implies
     */
    public static ColumnReader open(final Function<String, MappedSegment> opener, final String relativeDir,
                                    final long elementCount, final Manifest.ColumnInfo info) {
        return new ColumnReader(opener, relativeDir, elementCount, info, null);
    }

    /**
     * Opens a companion column, such as a vertex-property identifier column, that shares {@code presence.bin} or
     * {@code ordinals.bin} with its parent. The parent must stay open while the companion is used, and closing either
     * does nothing to the other's shared segments.
     */
    public static ColumnReader openCompanion(final Function<String, MappedSegment> opener, final String relativeDir,
                                             final ColumnReader parent, final Manifest.ColumnInfo info) {
        Objects.requireNonNull(parent);
        return new ColumnReader(opener, relativeDir, parent.elementCount, info, parent);
    }

    public Manifest.ColumnInfo info() {
        return info;
    }

    /**
     * The number of elements the column covers, present or not.
     */
    public long elementCount() {
        return elementCount;
    }

    public long presentCount() {
        return info.presentCount();
    }

    /**
     * The number of entries in the value files: the element count of a dense column and the present count of a sparse
     * one. Entry indexes run from 0 to this count exclusive.
     */
    public long entryCount() {
        return dense ? elementCount : info.presentCount();
    }

    /**
     * The segments this reader maps itself, that is excluding those shared with a parent.
     */
    public List<MappedSegment> segments() {
        return segments;
    }

    /**
     * The index of the element's entry in the value files, or -1 when the element has no value. This is the element
     * ordinal for a dense column and the rank among present elements for a sparse one.
     *
     * @throws IndexOutOfBoundsException if the ordinal is not in {@code [0, elementCount)}
     */
    public long entryIndex(final long ordinal) {
        Objects.checkIndex(ordinal, elementCount);
        if (dense) {
            if (presence == null) return ordinal;
            return (presence.getLong(ordinal >>> 6) & (1L << (ordinal & 63))) != 0 ? ordinal : -1;
        }
        int lo = 0;
        int hi = (int) info.presentCount() - 1;
        while (lo <= hi) {
            final int mid = (lo + hi) >>> 1;
            final int v = ordinals.getInt(mid);
            if (v < ordinal) {
                lo = mid + 1;
            } else if (v > ordinal) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    /**
     * Whether the element has a value.
     */
    public boolean isPresent(final long ordinal) {
        return entryIndex(ordinal) >= 0;
    }

    /**
     * Whether the element has an entry that is a null value. False for an element with no entry.
     */
    public boolean isNull(final long ordinal) {
        final long entry = entryIndex(ordinal);
        return entry >= 0 && isNullAt(entry);
    }

    /**
     * Whether the entry is a null value.
     *
     * @throws IndexOutOfBoundsException if the entry index is not in {@code [0, entryCount())}
     */
    public boolean isNullAt(final long entryIndex) {
        Objects.checkIndex(entryIndex, entryCount());
        return nulls != null && (nulls.getLong(entryIndex >>> 6) & (1L << (entryIndex & 63))) != 0;
    }

    /**
     * The type of the element's value, or null when it has none or it is null.
     */
    public ValueType type(final long ordinal) {
        final long entry = entryIndex(ordinal);
        return entry < 0 ? null : typeAt(entry);
    }

    /**
     * The type of the entry's value, or null for a null value. For a dense column the entry of an absent element has
     * no meaningful type, and is null in a mixed-type column.
     *
     * @throws IndexOutOfBoundsException if the entry index is not in {@code [0, entryCount())}
     */
    public ValueType typeAt(final long entryIndex) {
        if (isNullAt(entryIndex)) return null;
        return typeOfEntry(entryIndex);
    }

    /**
     * The element's decoded value, or null when it has none or it is null.
     */
    public Object get(final long ordinal) {
        final long entry = entryIndex(ordinal);
        return entry < 0 ? null : getAt(entry);
    }

    /**
     * The entry's decoded value, or null for a null value.
     *
     * @throws IndexOutOfBoundsException if the entry index is not in {@code [0, entryCount())}
     */
    public Object getAt(final long entryIndex) {
        final ValueType type = typeAt(entryIndex);
        if (type == null) return null;
        if (fixed) return ValueCodec.fixedValue(type, fixedBits(entryIndex));
        final byte[] bytes = variableBytes(entryIndex);
        return ValueCodec.decode(type, bytes, 0, bytes.length);
    }

    /**
     * The element's canonical encoded bytes as produced by {@link ValueCodec#encode(ValueType, Object)}, or null when
     * it has none or it is null. Use {@link #type(long)} for the type.
     */
    public byte[] encoded(final long ordinal) {
        final long entry = entryIndex(ordinal);
        if (entry < 0 || isNullAt(entry)) return null;
        return fixed ? ValueCodec.fixedBitsToBytes(singleType, fixedBits(entry)) : variableBytes(entry);
    }

    /**
     * Whether the element has a value of the given type with exactly the given encoded bytes. This is the identity
     * comparison used by non-exact identifier indexes. A null value matches nothing.
     */
    public boolean matches(final long ordinal, final ValueType type, final byte[] encoded) {
        final long entry = entryIndex(ordinal);
        if (entry < 0 || typeAt(entry) != type) return false;
        if (fixed) return Arrays.equals(ValueCodec.fixedBitsToBytes(singleType, fixedBits(entry)), encoded);
        return Arrays.equals(variableBytes(entry), encoded);
    }

    /**
     * Whether both elements have a value of the same type with the same encoded bytes.
     */
    public boolean sameValue(final long ordinalA, final long ordinalB) {
        final ValueType type = type(ordinalA);
        return type != null && matches(ordinalB, type, encoded(ordinalA));
    }

    // ---------------------------------------------------------------- primitive access for native execution

    /**
     * The one value type of the column, or null when it is mixed or holds no values.
     */
    public ValueType singleType() {
        return singleType;
    }

    /**
     * Whether values are stored fixed-width, so {@link #rawBitsAt(long)} applies.
     */
    public boolean isFixed() {
        return fixed;
    }

    /**
     * Whether the column has an entry for every element position ({@code entryIndex == ordinal}); a dense column may
     * still have absent positions, see {@link #hasPresenceWords()}.
     */
    public boolean isDense() {
        return dense;
    }

    /**
     * Whether the column holds more than one value type and therefore has a {@code types.bin}.
     */
    public boolean isMixed() {
        return mixed;
    }

    /**
     * Whether some entry is a null value, that is whether {@link #nullWord(long)} can return a nonzero word.
     */
    public boolean hasNulls() {
        return nulls != null;
    }

    /**
     * Whether the column is dense and not every element has a value, so {@link #presenceWord(long)} reads a bitmap. A
     * dense column without a bitmap has every element present, and a sparse column has no bitmap.
     */
    public boolean hasPresenceWords() {
        return presence != null;
    }

    /**
     * The number of 64-bit words of the presence bitmap, {@code ceil(elementCount / 64)}. Meaningful for dense columns.
     */
    public long presenceWordCount() {
        return (elementCount + 63) >>> 6;
    }

    /**
     * A word of the presence bitmap of a dense column, in which bit {@code i} of word {@code w} is element
     * {@code 64 * w + i}. Returns all ones, less the bits beyond the element count, when every element is present.
     *
     * @throws UnsupportedOperationException for a sparse column
     * @throws IndexOutOfBoundsException     if the word index is not in {@code [0, presenceWordCount())}
     */
    public long presenceWord(final long wordIndex) {
        if (!dense) throw new UnsupportedOperationException("Column " + relativeDir + " is sparse");
        Objects.checkIndex(wordIndex, presenceWordCount());
        if (presence != null) return presence.getLong(wordIndex);
        final long remaining = elementCount - (wordIndex << 6);
        return remaining >= 64 ? -1L : (1L << remaining) - 1;
    }

    /**
     * A word of the null bitmap, in which bit {@code i} of word {@code w} is the entry {@code 64 * w + i}. Zero for
     * every word when {@link #hasNulls()} is false.
     *
     * @throws IndexOutOfBoundsException if the word index is not in {@code [0, ceil(entryCount() / 64))}
     */
    public long nullWord(final long wordIndex) {
        Objects.checkIndex(wordIndex, (entryCount() + 63) >>> 6);
        return nulls == null ? 0 : nulls.getLong(wordIndex);
    }

    /**
     * The raw stored bits of a fixed-width entry, sign-extended from the column's value width: the numeric value for
     * {@code BYTE}, {@code SHORT}, {@code INT} and {@code LONG}, the character for {@code CHAR}, {@code 0} or
     * {@code 1} for {@code BOOLEAN}, and the IEEE bits for {@code FLOAT} (low 32 bits) and {@code DOUBLE}. The bits of
     * a null entry and of an absent position of a dense column are zero.
     *
     * @throws UnsupportedOperationException if the column is not {@link #isFixed() fixed-width}
     * @throws IndexOutOfBoundsException     if the entry index is not in {@code [0, entryCount())}
     */
    public long rawBitsAt(final long entryIndex) {
        if (!fixed) throw new UnsupportedOperationException("Column " + relativeDir + " is not fixed-width");
        return fixedBits(entryIndex);
    }

    /**
     * The element ordinal of an entry: the entry index itself for a dense column and the element with the
     * {@code entryIndex}th value for a sparse one.
     *
     * @throws IndexOutOfBoundsException if the entry index is not in {@code [0, entryCount())}
     */
    public long ordinalAt(final long entryIndex) {
        Objects.checkIndex(entryIndex, entryCount());
        return dense ? entryIndex : ordinals.getInt(entryIndex);
    }

    /**
     * The entry's canonical encoded bytes, see {@link #encoded(long)}, or null for a null value.
     *
     * @throws IndexOutOfBoundsException if the entry index is not in {@code [0, entryCount())}
     */
    public byte[] encodedAt(final long entryIndex) {
        if (isNullAt(entryIndex)) return null;
        return fixed ? ValueCodec.fixedBitsToBytes(singleType, fixedBits(entryIndex)) : variableBytes(entryIndex);
    }

    /**
     * A cursor for looking up ascending ordinals without binary searches.
     */
    public ColumnCursor cursor() {
        return new ColumnCursor(this);
    }

    private ValueType typeOfEntry(final long entry) {
        if (!mixed) return singleType;
        final byte code = types.getByte(entry);
        if (code == 0) return null;
        final ValueType type = ValueCodec.typeOfCode(code);
        if (type == null) throw corrupt("entry " + entry + " has an invalid type code");
        return type;
    }

    // only the low singleType.width() bytes are significant
    private long fixedBits(final long entry) {
        switch (singleType.width()) {
            case 1:
                return values.getByte(entry);
            case 2:
                return values.getShort(entry);
            case 4:
                return values.getInt(entry);
            default:
                return values.getLong(entry);
        }
    }

    private byte[] variableBytes(final long entry) {
        final long start = offsets.getLong(entry);
        final long end = offsets.getLong(entry + 1);
        final byte[] out = new byte[Math.toIntExact(end - start)];
        data.getBytes(start, out, 0, out.length);
        return out;
    }

    /**
     * Recomputes the checksum of every segment this reader maps itself.
     *
     * @throws java.io.UncheckedIOException if one does not match its header
     */
    public void verifyChecksums() {
        for (final MappedSegment s : segments) s.verifyChecksum();
    }

    @Override
    public void close() {
        for (final MappedSegment s : segments) s.close();
    }
}
