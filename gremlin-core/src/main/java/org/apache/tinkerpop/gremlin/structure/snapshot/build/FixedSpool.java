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

import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentWriter;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * A scratch spool of int32 or int64 values, the hybrid builder's counterpart of a scratch {@link SegmentWriter}. While
 * the budget grants it chunks the values are held in heap and nothing is written; once it does not, or when the budget
 * evicts the spool as its largest holder, the values move to a segment file for good. Written once and read back
 * sequentially.
 */
final class FixedSpool implements AutoCloseable, BuildBudget.Spillable {

    private static final int CHUNK_BYTES = 64 * 1024;

    private final Path path;
    private final int width;
    private final int ioBuffer;
    private final BuildBudget budget;
    private final int perChunk;

    private boolean inMemory;
    private List<int[]> intChunks;
    private List<long[]> longChunks;
    private long count;
    private long heldBytes;
    private SegmentWriter writer;
    private boolean finished;

    /**
     * @param width 4 or 8
     */
    FixedSpool(final Path path, final int width, final int ioBuffer, final BuildBudget budget) {
        if (width != 4 && width != 8) throw new IllegalArgumentException("Unsupported width " + width);
        this.path = path;
        this.width = width;
        this.ioBuffer = ioBuffer;
        this.budget = budget;
        this.perChunk = CHUNK_BYTES / width;
        this.inMemory = budget.total() > 0;
        if (inMemory) {
            if (width == 4) intChunks = new ArrayList<>();
            else longChunks = new ArrayList<>();
            budget.register(this);
        } else {
            writer = SegmentWriter.create(path, width, ioBuffer);
        }
    }

    Path path() {
        return path;
    }

    boolean isInMemory() {
        return inMemory;
    }

    long count() {
        return inMemory ? count : writer.count();
    }

    @Override
    public long heldBytes() {
        return heldBytes;
    }

    void writeInt(final int value) {
        if (inMemory) {
            final int slot = (int) (count % perChunk);
            if (slot == 0 && !grow()) {
                writer.writeInt(value);
                return;
            }
            intChunks.get(intChunks.size() - 1)[slot] = value;
            count++;
            return;
        }
        writer.writeInt(value);
    }

    void writeLong(final long value) {
        if (inMemory) {
            final int slot = (int) (count % perChunk);
            if (slot == 0 && !grow()) {
                writer.writeLong(value);
                return;
            }
            longChunks.get(longChunks.size() - 1)[slot] = value;
            count++;
            return;
        }
        writer.writeLong(value);
    }

    // adds a chunk; false when the spool had to move to its file instead, in which case the value goes there
    private boolean grow() {
        if (!budget.reserve(CHUNK_BYTES, this)) {
            spill();
            return false;
        }
        heldBytes += CHUNK_BYTES;
        if (width == 4) intChunks.add(new int[perChunk]);
        else longChunks.add(new long[perChunk]);
        return true;
    }

    @Override
    public void spill() {
        if (!inMemory) return;
        inMemory = false;
        budget.unregister(this);
        writer = SegmentWriter.create(path, width, ioBuffer);
        if (width == 4) {
            long remaining = count;
            for (final int[] chunk : intChunks) {
                final int n = (int) Math.min(perChunk, remaining);
                for (int i = 0; i < n; i++) writer.writeInt(chunk[i]);
                remaining -= n;
            }
        } else {
            long remaining = count;
            for (final long[] chunk : longChunks) {
                final int n = (int) Math.min(perChunk, remaining);
                for (int i = 0; i < n; i++) writer.writeLong(chunk[i]);
                remaining -= n;
            }
        }
        intChunks = null;
        longChunks = null;
        count = 0;
        budget.release(heldBytes);
        heldBytes = 0;
        if (finished) writer.finish();
    }

    void finish() {
        if (finished) return;
        finished = true;
        if (!inMemory) writer.finish();
    }

    /**
     * Opens the finished spool for sequential reading.
     */
    Reader reader(final int readBufferBytes) {
        if (!finished) throw new IllegalStateException("Spool " + path + " is not finished");
        return inMemory ? new Reader(this) : new Reader(SegmentReader.open(path, readBufferBytes));
    }

    /**
     * The path of the spool as a finished segment file, writing the values out first if they are held in heap.
     */
    Path materialize() {
        if (!finished) throw new IllegalStateException("Spool " + path + " is not finished");
        spill();
        return path;
    }

    /**
     * Frees the heap the spool holds and returns it to the budget.
     */
    void release() {
        if (!inMemory) return;
        inMemory = false;
        budget.unregister(this);
        intChunks = null;
        longChunks = null;
        count = 0;
        budget.release(heldBytes);
        heldBytes = 0;
    }

    @Override
    public void close() {
        if (writer != null) writer.close();
        release();
    }

    /**
     * A sequential reader over a finished spool.
     */
    static final class Reader implements AutoCloseable {
        private final SegmentReader file;
        private final List<int[]> ints;
        private final List<long[]> longs;
        private final int perChunk;
        private long position;

        private Reader(final SegmentReader file) {
            this.file = file;
            this.ints = null;
            this.longs = null;
            this.perChunk = 0;
        }

        private Reader(final FixedSpool spool) {
            this.file = null;
            this.ints = spool.intChunks;
            this.longs = spool.longChunks;
            this.perChunk = spool.perChunk;
        }

        int readInt() {
            if (file != null) return file.readInt();
            final int value = ints.get((int) (position / perChunk))[(int) (position % perChunk)];
            position++;
            return value;
        }

        long readLong() {
            if (file != null) return file.readLong();
            final long value = longs.get((int) (position / perChunk))[(int) (position % perChunk)];
            position++;
            return value;
        }

        @Override
        public void close() {
            if (file != null) file.close();
        }
    }
}
