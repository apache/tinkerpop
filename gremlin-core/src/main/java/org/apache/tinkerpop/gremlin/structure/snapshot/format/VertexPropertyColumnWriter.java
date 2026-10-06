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
 * Writes the column of one vertex property key together with its vertex-property identifier column, in one pass. The
 * value column is written to {@link SegmentPaths#vertexPropertyDir(int)} and the identifier column, which has the same
 * entries in the same positions, to {@link SegmentPaths#vertexPropertyIdsDir(int)} as a companion that reuses the
 * parent's {@code presence.bin} or {@code ordinals.bin}.
 * <p/>
 * Both {@link Manifest.ColumnInfo}s must come from statistics over the same entries, so they have the same present
 * count and therefore the same density. The identifier types are usually different from the value types. The value
 * column may contain nulls, see {@link ColumnWriter#appendNull}, but identifiers never are null.
 * <p/>
 * The same writer serves both vertex property layouts. In the single layout the element count is the vertex count and
 * the ordinal of an append is a vertex ordinal. In the multi layout the element count is the key's vertex-property
 * count, every ordinal from 0 is appended so both columns are dense and complete and write no {@code presence.bin},
 * and the ordinal of an append is the vertex-property ordinal.
 */
public final class VertexPropertyColumnWriter implements AutoCloseable {

    private final ColumnWriter values;
    private final ColumnWriter ids;
    private List<Manifest.SegmentInfo> segments;

    private VertexPropertyColumnWriter(final ColumnWriter values, final ColumnWriter ids) {
        this.values = values;
        this.ids = ids;
    }

    /**
     * Creates the writer for one vertex property key.
     *
     * @param resolver     maps a path relative to the bundle root (or the scratch root) to a file, for example
     *                     {@code BuildDirectory::segmentPath}
     * @param keyCode      the property-key code
     * @param elementCount the number of vertices
     * @param valueInfo    the value column's info
     * @param idInfo       the identifier column's info, with the same present count as {@code valueInfo}
     */
    public static VertexPropertyColumnWriter create(final Function<String, Path> resolver, final int keyCode,
                                                    final long elementCount, final Manifest.ColumnInfo valueInfo,
                                                    final Manifest.ColumnInfo idInfo) {
        if (valueInfo.presentCount() != idInfo.presentCount()) {
            throw new IllegalArgumentException("Vertex property key " + keyCode + " has " + valueInfo.presentCount()
                    + " values but " + idInfo.presentCount() + " identifiers");
        }
        final ColumnWriter v = ColumnWriter.create(resolver, SegmentPaths.vertexPropertyDir(keyCode), elementCount,
                valueInfo);
        try {
            final ColumnWriter i = ColumnWriter.createCompanion(resolver, SegmentPaths.vertexPropertyIdsDir(keyCode),
                    elementCount, idInfo);
            return new VertexPropertyColumnWriter(v, i);
        } catch (RuntimeException e) {
            v.close();
            throw e;
        }
    }

    public Manifest.ColumnInfo valueInfo() {
        return values.info();
    }

    public Manifest.ColumnInfo idInfo() {
        return ids.info();
    }

    /**
     * Appends a vertex property.
     *
     * @param ordinal the vertex ordinal, greater than the previous one
     * @param id      the vertex-property identifier
     * @param value   the property value, which may be null
     * @throws UnsupportedSnapshotDataException if the identifier is null or either is of an unsupported class
     */
    public void append(final long ordinal, final Object id, final Object value) {
        ids.append(ordinal, id);
        values.append(ordinal, value);
    }

    /**
     * Appends an already encoded vertex property, for example one replayed from a spool. See
     * {@link ColumnWriter#appendEncoded}.
     */
    public void appendEncoded(final long ordinal,
                              final ValueType idType, final byte[] idBuf, final int idOffset, final int idLength,
                              final ValueType valueType, final byte[] valueBuf, final int valueOffset,
                              final int valueLength) {
        ids.appendEncoded(ordinal, idType, idBuf, idOffset, idLength);
        values.appendEncoded(ordinal, valueType, valueBuf, valueOffset, valueLength);
    }

    /**
     * Appends an already encoded vertex property whose value is null.
     */
    public void appendNullValueEncoded(final long ordinal, final ValueType idType, final byte[] idBuf,
                                       final int idOffset, final int idLength) {
        ids.appendEncoded(ordinal, idType, idBuf, idOffset, idLength);
        values.appendNull(ordinal);
    }

    /**
     * Completes both columns.
     *
     * @return the segments of both columns, in ascending path order
     */
    public List<Manifest.SegmentInfo> finish() {
        if (segments == null) {
            final List<Manifest.SegmentInfo> all = new ArrayList<>(values.finish());
            all.addAll(ids.finish());
            all.sort(Comparator.comparing(Manifest.SegmentInfo::path));
            segments = all;
        }
        return segments;
    }

    @Override
    public void close() {
        values.close();
        ids.close();
    }
}
