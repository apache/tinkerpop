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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

/**
 * Writes one column directory, see the Columns section of the spike document. It serves both builders because it only
 * ever appends: entries are presented in strictly ascending ordinal order and the writer streams them through small
 * buffers, so its heap use is independent of the column size. The materializing builder iterates its in-heap entries
 * and the streaming builder replays its spool, and both make the same calls. The output depends only on the entries
 * and the {@link Manifest.ColumnInfo}, so both builders produce identical bytes.
 * <p/>
 * The {@link Manifest.ColumnInfo} must be known before the first append, which is why both builders collect
 * {@link ColumnStats} during the scan. The writer creates exactly the files its encoding needs:
 * <ul>
 *     <li>dense: {@code presence.bin}, omitted when every element has a value;</li>
 *     <li>sparse: {@code ordinals.bin} (int32);</li>
 *     <li>fixed: {@code values.bin} with the value width;</li>
 *     <li>variable: {@code offsets.bin} (int64, entries + 1 of them) and {@code data.bin};</li>
 *     <li>mixed types: additionally {@code types.bin} with one type code per entry, 0 for an absent or null entry;</li>
 *     <li>at least one null value: additionally {@code nulls.bin}, a bitmap of int64 words like {@code presence.bin}
 *     with a bit per entry, that is per element of a dense column and per present value of a sparse one.</li>
 * </ul>
 * A dense column has one entry per element, with a zero value or an empty range at absent positions. A sparse column
 * has one entry per present value. A null value is a present entry like any other, appended with {@link #appendNull}
 * (or {@link #append} of null), that holds a zero value or an empty range and is marked in {@code nulls.bin}. The
 * null count of the {@link Manifest.ColumnInfo} must equal the number of nulls appended.
 * <p/>
 * A <em>companion</em> column (see {@link #createCompanion}) writes only the value files and skips {@code presence.bin}
 * and {@code ordinals.bin}. It is used for the vertex-property identifier column, which shares those files with its
 * parent value column. See {@link VertexPropertyColumnWriter} for the combined writer.
 * <p/>
 * Values must be appended in strictly ascending ordinal order and the number of appended values, nulls included, must
 * equal {@code info.presentCount()} when {@link #finish()} is called. I/O failures are {@code UncheckedIOException}.
 * Instances are not thread-safe.
 */
public final class ColumnWriter implements AutoCloseable {

    private final String relativeDir;
    private final long elementCount;
    private final Manifest.ColumnInfo info;
    private final boolean dense;
    private final boolean fixed;
    private final boolean mixed;
    private final int fixedWidth;
    private final boolean[] allowed = new boolean[256];
    private final List<Manifest.SegmentInfo> segments = new ArrayList<>();

    private SegmentWriter presence;
    private SegmentWriter ordinals;
    private SegmentWriter values;
    private SegmentWriter offsets;
    private SegmentWriter data;
    private SegmentWriter types;
    private SegmentWriter nulls;

    private long lastOrdinal = -1;
    // dense columns: the next position whose entry has not been written to values/offsets/types
    private long nextPosition;
    private long dataLength;
    private long appended;
    // presence bitmap: words written so far and the pending word, which has index presenceWordsWritten
    private long presenceWordsWritten;
    private long presenceWord;
    // null bitmap, same scheme as the presence bitmap but indexed by entry
    private long nulled;
    private long nullWordsWritten;
    private long nullWord;
    private boolean finished;

    private ColumnWriter(final Function<String, Path> resolver, final String relativeDir, final long elementCount,
                         final Manifest.ColumnInfo info, final boolean companion) {
        this.relativeDir = relativeDir;
        this.elementCount = elementCount;
        this.info = info;
        this.dense = isDense(info.encoding());
        this.fixed = isFixed(info.encoding());
        this.mixed = info.valueTypes().size() > 1;
        if (info.presentCount() < 0 || info.presentCount() > elementCount) {
            throw new IllegalArgumentException("Column " + relativeDir + " has " + info.presentCount()
                    + " values for " + elementCount + " elements");
        }
        if (dense != ColumnStats.isDense(elementCount, info.presentCount())) {
            throw new IllegalArgumentException("Column " + relativeDir + " encoding " + info.encoding()
                    + " does not match " + info.presentCount() + " of " + elementCount + " elements");
        }
        if (fixed && (info.valueTypes().size() != 1 || !info.valueTypes().get(0).isFixedWidth())) {
            throw new IllegalArgumentException("Column " + relativeDir + " encoding " + info.encoding()
                    + " requires exactly one fixed-width type, found " + info.valueTypes());
        }
        if (info.nullCount() < 0 || info.nullCount() > info.presentCount()) {
            throw new IllegalArgumentException("Column " + relativeDir + " has " + info.nullCount()
                    + " nulls among " + info.presentCount() + " values");
        }
        this.fixedWidth = fixed ? info.valueTypes().get(0).width() : 0;
        for (final ValueType t : info.valueTypes()) allowed[t.code() & 0xff] = true;

        try {
            if (!companion) {
                if (dense && info.presentCount() < elementCount) {
                    presence = open(resolver, SegmentPaths.PRESENCE, 8);
                } else if (!dense) {
                    ordinals = open(resolver, SegmentPaths.ORDINALS, 4);
                }
            }
            if (fixed) {
                values = open(resolver, SegmentPaths.VALUES, fixedWidth);
            } else {
                offsets = open(resolver, SegmentPaths.OFFSETS, 8);
                data = open(resolver, SegmentPaths.DATA, 1);
            }
            if (mixed) types = open(resolver, SegmentPaths.TYPES, 1);
            if (info.nullCount() > 0) nulls = open(resolver, SegmentPaths.NULLS, 8);
        } catch (RuntimeException e) {
            closeQuietly();
            throw e;
        }
    }

    private SegmentWriter open(final Function<String, Path> resolver, final String fileName, final int width) {
        return SegmentWriter.create(resolver.apply(SegmentPaths.file(relativeDir, fileName)), width);
    }

    /**
     * Creates a column writer.
     *
     * @param resolver     maps a path relative to the bundle root (or the scratch root) to a file, creating missing
     *                     parent directories, for example {@code BuildDirectory::segmentPath}
     * @param relativeDir  the column directory relative to that root, for example
     *                     {@link SegmentPaths#vertexPropertyDir(int)}; also recorded in the segment infos
     * @param elementCount the number of elements of the column's kind: all vertices, or all edges
     * @param info         the encoding, value types and present count, normally from {@link ColumnStats#toInfo(long)}
     */
    public static ColumnWriter create(final Function<String, Path> resolver, final String relativeDir,
                                      final long elementCount, final Manifest.ColumnInfo info) {
        return new ColumnWriter(resolver, relativeDir, elementCount, info, false);
    }

    /**
     * Creates a companion column writer that writes no {@code presence.bin} or {@code ordinals.bin}. The caller is
     * responsible for the parent column having the same present count, element count and positions.
     */
    public static ColumnWriter createCompanion(final Function<String, Path> resolver, final String relativeDir,
                                               final long elementCount, final Manifest.ColumnInfo info) {
        return new ColumnWriter(resolver, relativeDir, elementCount, info, true);
    }

    static boolean isDense(final ColumnEncoding encoding) {
        return encoding == ColumnEncoding.DENSE_FIXED || encoding == ColumnEncoding.DENSE_VARIABLE;
    }

    static boolean isFixed(final ColumnEncoding encoding) {
        return encoding == ColumnEncoding.DENSE_FIXED || encoding == ColumnEncoding.SPARSE_FIXED;
    }

    /**
     * The description of this column for the manifest.
     */
    public Manifest.ColumnInfo info() {
        return info;
    }

    /**
     * The number of values appended so far.
     */
    public long appendedCount() {
        return appended;
    }

    /**
     * Appends a value, encoding it with {@link ValueCodec}. The value's type must be one of {@code info.valueTypes()}.
     * A null value is appended as by {@link #appendNull}.
     *
     * @param ordinal the element ordinal, greater than the previous one and less than the element count
     * @throws UnsupportedSnapshotDataException if the value is of an unsupported class
     */
    public void append(final long ordinal, final Object value) {
        if (value == null) {
            appendNull(ordinal);
            return;
        }
        final ValueType type = ValueCodec.requireType(value, "value of column " + relativeDir, null);
        begin(ordinal, type);
        if (fixed) {
            writeFixed(ValueCodec.fixedBits(type, value));
        } else {
            final byte[] encoded = ValueCodec.encode(type, value);
            writeVariable(type, encoded, 0, encoded.length);
        }
        appended++;
    }

    /**
     * Appends a null value: a present entry with a zero value or an empty range, marked in {@code nulls.bin}. It
     * records no value type, so it needs no entry in {@code info.valueTypes()}, and it counts towards the present count
     * and the null count of the column's info.
     *
     * @param ordinal the element ordinal, greater than the previous one and less than the element count
     * @throws IllegalStateException if the info's null count is already reached
     */
    public void appendNull(final long ordinal) {
        if (nulled >= info.nullCount()) {
            throw new IllegalStateException("Column " + relativeDir + " exceeds its null count " + info.nullCount());
        }
        final long entry = dense ? ordinal : appended;
        begin(ordinal, null);
        writeAbsent();
        markNull(entry);
        nulled++;
        appended++;
    }

    private void markNull(final long entry) {
        final long word = entry >>> 6;
        while (nullWordsWritten < word) {
            nulls.writeLong(nullWord);
            nullWord = 0;
            nullWordsWritten++;
        }
        nullWord |= 1L << (entry & 63);
    }

    /**
     * Appends an already encoded value, for example one replayed from a spool. For a fixed-width type the payload must
     * be exactly its width in little-endian order, as produced by {@link ValueCodec#encode(ValueType, Object)}.
     *
     * @param ordinal the element ordinal, greater than the previous one and less than the element count
     * @param type    the value's type, one of {@code info.valueTypes()}
     */
    public void appendEncoded(final long ordinal, final ValueType type, final byte[] payload, final int offset,
                              final int length) {
        begin(ordinal, type);
        if (fixed) {
            writeFixed(ValueCodec.fixedBitsFromBytes(type, payload, offset, length));
        } else {
            writeVariable(type, payload, offset, length);
        }
        appended++;
    }

    // a null type is a null value
    private void begin(final long ordinal, final ValueType type) {
        if (finished) throw new IllegalStateException("Column " + relativeDir + " is already finished");
        if (ordinal <= lastOrdinal || ordinal >= elementCount) {
            throw new IllegalArgumentException("Ordinal " + ordinal + " of column " + relativeDir + " is not in ("
                    + lastOrdinal + ", " + elementCount + ")");
        }
        if (type != null && !allowed[type.code() & 0xff]) {
            throw new IllegalArgumentException("Type " + type + " is not among the types " + info.valueTypes()
                    + " of column " + relativeDir);
        }
        if (appended >= info.presentCount()) {
            throw new IllegalStateException("Column " + relativeDir + " exceeds its present count "
                    + info.presentCount());
        }
        lastOrdinal = ordinal;
        if (dense) {
            while (nextPosition < ordinal) {
                writeAbsent();
                nextPosition++;
            }
            nextPosition = ordinal + 1;
            if (presence != null) markPresent(ordinal);
        } else if (ordinals != null) {
            ordinals.writeInt((int) ordinal);
        }
    }

    private void markPresent(final long ordinal) {
        final long word = ordinal >>> 6;
        while (presenceWordsWritten < word) {
            presence.writeLong(presenceWord);
            presenceWord = 0;
            presenceWordsWritten++;
        }
        presenceWord |= 1L << (ordinal & 63);
    }

    private void writeFixed(final long bits) {
        switch (fixedWidth) {
            case 1:
                values.writeByte((int) bits);
                break;
            case 2:
                values.writeShort((int) bits);
                break;
            case 4:
                values.writeInt((int) bits);
                break;
            default:
                values.writeLong(bits);
                break;
        }
    }

    private void writeVariable(final ValueType type, final byte[] payload, final int offset, final int length) {
        if (type.isFixedWidth() && length != type.width()) {
            throw new IllegalArgumentException("Type " + type + " requires " + type.width() + " bytes, got " + length);
        }
        offsets.writeLong(dataLength);
        data.writeBytes(payload, offset, length);
        dataLength += length;
        if (mixed) types.writeByte(type.code());
    }

    // an absent position of a dense column
    private void writeAbsent() {
        if (fixed) {
            switch (fixedWidth) {
                case 1:
                    values.writeByte(0);
                    break;
                case 2:
                    values.writeShort(0);
                    break;
                case 4:
                    values.writeInt(0);
                    break;
                default:
                    values.writeLong(0);
                    break;
            }
        } else {
            offsets.writeLong(dataLength);
            if (mixed) types.writeByte(0);
        }
    }

    /**
     * Completes the column: writes the remaining absent positions, the presence bitmap and the final offset, and
     * finalizes every segment header. Idempotent.
     *
     * @return the segments written, in ascending path order
     * @throws IllegalStateException if the number of appended values differs from the present count
     */
    public List<Manifest.SegmentInfo> finish() {
        if (finished) return segments;
        if (appended != info.presentCount()) {
            throw new IllegalStateException("Column " + relativeDir + " received " + appended + " values, expected "
                    + info.presentCount());
        }
        if (dense) {
            while (nextPosition < elementCount) {
                writeAbsent();
                nextPosition++;
            }
        }
        if (presence != null) {
            final long totalWords = (elementCount + 63) >>> 6;
            while (presenceWordsWritten < totalWords) {
                presence.writeLong(presenceWord);
                presenceWord = 0;
                presenceWordsWritten++;
            }
        }
        if (!fixed) offsets.writeLong(dataLength);
        if (nulled != info.nullCount()) {
            throw new IllegalStateException("Column " + relativeDir + " received " + nulled + " nulls, expected "
                    + info.nullCount());
        }
        if (nulls != null) {
            final long totalWords = ((dense ? elementCount : info.presentCount()) + 63) >>> 6;
            while (nullWordsWritten < totalWords) {
                nulls.writeLong(nullWord);
                nullWord = 0;
                nullWordsWritten++;
            }
        }
        finished = true;
        add(presence, SegmentPaths.PRESENCE);
        add(ordinals, SegmentPaths.ORDINALS);
        add(values, SegmentPaths.VALUES);
        add(offsets, SegmentPaths.OFFSETS);
        add(data, SegmentPaths.DATA);
        add(types, SegmentPaths.TYPES);
        add(nulls, SegmentPaths.NULLS);
        segments.sort(Comparator.comparing(Manifest.SegmentInfo::path));
        return segments;
    }

    private void add(final SegmentWriter writer, final String fileName) {
        if (writer == null) return;
        writer.finish();
        segments.add(writer.info(SegmentPaths.file(relativeDir, fileName)));
    }

    /**
     * The segments of a finished column, see {@link #finish()}.
     */
    public List<Manifest.SegmentInfo> segments() {
        if (!finished) throw new IllegalStateException("Column " + relativeDir + " is not finished");
        return segments;
    }

    /**
     * Releases the files of a column that was not finished, for example after a failure. The partial files are left
     * for the build directory cleanup. Does nothing after {@link #finish()}.
     */
    @Override
    public void close() {
        if (!finished) closeQuietly();
    }

    private void closeQuietly() {
        for (final SegmentWriter w : new SegmentWriter[]{presence, ordinals, values, offsets, data, types, nulls}) {
            if (w == null) continue;
            try {
                w.close();
            } catch (RuntimeException ignored) {
                // the build is failing and its directory will be deleted
            }
        }
    }
}
