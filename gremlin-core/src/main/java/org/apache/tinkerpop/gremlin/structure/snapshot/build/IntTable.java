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

import org.apache.tinkerpop.gremlin.structure.snapshot.format.MappedSegment;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.Manifest;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentWriter;

import java.nio.file.Path;

/**
 * A zero-initialized int32 array that is either an {@code int[]} in heap or a memory-mapped scratch or output segment.
 * The hybrid builder allocates degree arrays, fill cursors and adjacency arrays in heap while the budget allows it.
 */
final class IntTable implements AutoCloseable {

    private final int[] heap;
    private final MappedSegment mapped;
    private final long count;
    private final BuildBudget budget;
    private final long heldBytes;
    private boolean closed;

    private IntTable(final int[] heap, final MappedSegment mapped, final long count, final BuildBudget budget,
                     final long heldBytes) {
        this.heap = heap;
        this.mapped = mapped;
        this.count = count;
        this.budget = budget;
        this.heldBytes = heldBytes;
    }

    /**
     * A table in heap when the budget grants its bytes, else a mapped file at the given path.
     */
    static IntTable allocate(final BuildBudget budget, final long count, final Path mappedPath) {
        final long bytes = count * Integer.BYTES;
        if (budget.total() > 0 && count <= Integer.MAX_VALUE - 8 && budget.reserve(bytes)) {
            return new IntTable(new int[(int) count], null, count, budget, bytes);
        }
        return new IntTable(null, MappedSegment.create(mappedPath, Integer.BYTES, count), count, budget, 0);
    }

    boolean isHeap() {
        return heap != null;
    }

    long count() {
        return count;
    }

    int get(final long index) {
        return heap != null ? heap[(int) index] : mapped.getInt(index);
    }

    void put(final long index, final int value) {
        if (heap != null) heap[(int) index] = value;
        else mapped.putInt(index, value);
    }

    /**
     * Completes the table as the segment at {@code relativePath}: a mapped table is finished in place, a table in heap
     * is written sequentially to the path. Either way the bytes are those of the heap builder's segment.
     */
    Manifest.SegmentInfo publish(final Path path, final String relativePath, final int ioBuffer) {
        if (heap == null) {
            mapped.finish();
            return mapped.info(relativePath);
        }
        try (SegmentWriter out = SegmentWriter.create(path, Integer.BYTES, ioBuffer)) {
            for (int i = 0; i < heap.length; i++) out.writeInt(heap[i]);
            out.finish();
            return out.info(relativePath);
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        if (mapped != null) mapped.close();
        if (heldBytes > 0) budget.release(heldBytes);
    }
}
