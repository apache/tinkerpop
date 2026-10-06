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

import java.util.Objects;

/**
 * Finds the range of vertex-property ordinals a vertex owns for one key, for ascending vertex ordinals, without a
 * binary search per vertex. For a multi key it walks {@code owner-ordinals.bin} sequentially (or indexes
 * {@code owner-offsets.bin} directly for dense owners); for a single key it wraps a {@link ColumnCursor} over the
 * value column. After {@link #seek(int)}, {@link #start()} and {@link #end()} give the half-open range, empty when the
 * vertex has no vertex property of the key. A vertex below the previous one is answered by a binary search. Instances
 * are stateful and not thread-safe; use one per scan.
 */
public final class OwnerCursor {

    private final ColumnCursor single;
    private final MappedSegment offsets;
    private final MappedSegment ordinals;
    private final int vertexCount;
    private long owner;
    private int lastVertex = -1;
    private long start;
    private long end;

    private OwnerCursor(final ColumnCursor single, final MappedSegment offsets, final MappedSegment ordinals,
                        final int vertexCount) {
        this.single = single;
        this.offsets = offsets;
        this.ordinals = ordinals;
        this.vertexCount = vertexCount;
    }

    /**
     * A cursor over a single-layout key, whose vertex-property ordinal is the entry index of the value column.
     */
    public static OwnerCursor ofColumn(final ColumnReader column) {
        return new OwnerCursor(column.cursor(), null, null, 0);
    }

    /**
     * A cursor over a multi-layout key.
     *
     * @param offsets  {@code owner-offsets.bin}
     * @param ordinals {@code owner-ordinals.bin}, or null for dense owners
     */
    public static OwnerCursor ofOwners(final MappedSegment offsets, final MappedSegment ordinals,
                                       final int vertexCount) {
        return new OwnerCursor(null, Objects.requireNonNull(offsets), ordinals, vertexCount);
    }

    /**
     * Positions the cursor at the vertex.
     *
     * @return true if the vertex has at least one vertex property of the key
     */
    public boolean seek(final int vertex) {
        if (single != null) {
            final long entry = single.seek(vertex);
            start = entry < 0 ? 0 : entry;
            end = entry < 0 ? 0 : entry + 1;
            return entry >= 0;
        }
        if (ordinals == null) {
            Objects.checkIndex(vertex, vertexCount);
            start = offsets.getLong(vertex);
            end = offsets.getLong(vertex + 1L);
            return end > start;
        }
        final long count = ordinals.count();
        if (vertex < lastVertex) owner = lowerBound(vertex);
        lastVertex = vertex;
        while (owner < count && ordinals.getInt(owner) < vertex) owner++;
        if (owner < count && ordinals.getInt(owner) == vertex) {
            start = offsets.getLong(owner);
            end = offsets.getLong(owner + 1);
            return true;
        }
        start = 0;
        end = 0;
        return false;
    }

    private long lowerBound(final int vertex) {
        long lo = 0;
        long hi = ordinals.count();
        while (lo < hi) {
            final long mid = (lo + hi) >>> 1;
            if (ordinals.getInt(mid) < vertex) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    /**
     * The first vertex-property ordinal of the last seeked vertex.
     */
    public long start() {
        return start;
    }

    /**
     * One past the last vertex-property ordinal of the last seeked vertex.
     */
    public long end() {
        return end;
    }
}
