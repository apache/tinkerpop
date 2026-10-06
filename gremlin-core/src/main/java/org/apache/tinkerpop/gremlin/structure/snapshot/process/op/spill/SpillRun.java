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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.MappedSegment;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.ScratchSpace;

import java.util.ArrayList;
import java.util.List;

/**
 * A sequence of length-prefixed records (a 4-byte length, then the payload) stored in width-1 scratch segments. Segments
 * grow geometrically, from 64 KiB to 8 MiB, so that small runs stay small. The write and read buffers are on the heap
 * and are reserved from the budget by the owner that creates them.
 */
final class SpillRun {

    private static final long FIRST_SEGMENT = 64L << 10;
    private static final long MAX_SEGMENT = 8L << 20;

    private final List<MappedSegment> segments = new ArrayList<>();
    private final List<Long> lengths = new ArrayList<>();
    private long records;
    private long bytes;

    long records() {
        return records;
    }

    long bytes() {
        return bytes;
    }

    /**
     * Deletes the files of the run. The run must not be read afterwards.
     */
    void discard(final ScratchSpace scratch) {
        for (final MappedSegment s : segments) scratch.discard(s);
        segments.clear();
        lengths.clear();
        records = 0;
        bytes = 0;
    }

    // ---------------------------------------------------------------- writer

    static final class Writer {

        private final CsrExecutionContext ctx;
        private final MemoryBudget budget;
        private final String owner;
        private final String name;
        private final SpillRun run = new SpillRun();
        private final byte[] buf;
        private int used;
        private MappedSegment current;
        private long currentCapacity;
        private long currentUsed;
        private boolean closed;

        /**
         * @param bufferBytes reserved from the budget under the owner until {@link #finish()} or {@link #abort()}
         */
        Writer(final CsrExecutionContext ctx, final String owner, final String name, final int bufferBytes) {
            this.ctx = ctx;
            this.budget = ctx.budget();
            this.owner = owner;
            this.name = name;
            budget.reserve(bufferBytes, owner);
            this.buf = new byte[bufferBytes];
        }

        void writeRecord(final byte[] payload) {
            writeRecord(payload, 0, payload.length);
        }

        void writeRecord(final byte[] payload, final int off, final int len) {
            final byte[] head = {(byte) (len >>> 24), (byte) (len >>> 16), (byte) (len >>> 8), (byte) len};
            write(head, 0, 4);
            write(payload, off, len);
            run.records++;
        }

        private void write(final byte[] src, final int off, final int len) {
            int o = off;
            int left = len;
            while (left > 0) {
                if (used == buf.length) flush();
                final int c = Math.min(left, buf.length - used);
                System.arraycopy(src, o, buf, used, c);
                used += c;
                o += c;
                left -= c;
            }
            run.bytes += len;
        }

        private void flush() {
            int o = 0;
            while (o < used) {
                if (current == null || currentUsed == currentCapacity) nextSegment();
                final int c = (int) Math.min(used - o, currentCapacity - currentUsed);
                current.putBytes(currentUsed, buf, o, c);
                currentUsed += c;
                o += c;
                run.lengths.set(run.lengths.size() - 1, currentUsed);
            }
            used = 0;
        }

        private void nextSegment() {
            currentCapacity = Math.min(MAX_SEGMENT, FIRST_SEGMENT << Math.min(run.segments.size(), 8));
            current = ctx.scratch().create(name, 1, currentCapacity);
            currentUsed = 0;
            run.segments.add(current);
            run.lengths.add(0L);
        }

        /**
         * Flushes, gives the buffer back and returns the run.
         */
        SpillRun finish() {
            if (!closed) {
                flush();
                closed = true;
                budget.release(buf.length, owner);
            }
            return run;
        }

        /**
         * Gives the buffer back and deletes what was written.
         */
        void abort() {
            if (!closed) {
                closed = true;
                budget.release(buf.length, owner);
            }
            run.discard(ctx.scratch());
        }
    }

    // ---------------------------------------------------------------- reader

    static final class Reader implements AutoCloseable {

        private final SpillRun run;
        private final MemoryBudget budget;
        private final String owner;
        private final byte[] buf;
        private int pos;
        private int limit;
        private int segment;
        private long segmentPos;
        private boolean closed;

        Reader(final CsrExecutionContext ctx, final SpillRun run, final String owner, final int bufferBytes) {
            this.run = run;
            this.budget = ctx.budget();
            this.owner = owner;
            budget.reserve(bufferBytes, owner);
            this.buf = new byte[bufferBytes];
        }

        private boolean fill() {
            while (segment < run.segments.size() && segmentPos >= run.lengths.get(segment)) {
                segment++;
                segmentPos = 0;
            }
            if (segment >= run.segments.size()) return false;
            final int c = (int) Math.min(buf.length, run.lengths.get(segment) - segmentPos);
            run.segments.get(segment).getBytes(segmentPos, buf, 0, c);
            segmentPos += c;
            pos = 0;
            limit = c;
            return true;
        }

        private boolean readFully(final byte[] dst, final int off, final int len) {
            int o = off;
            int left = len;
            while (left > 0) {
                if (pos == limit && !fill()) {
                    if (left == len) return false;
                    throw new IllegalStateException("A spill run ends inside a record");
                }
                final int c = Math.min(left, limit - pos);
                System.arraycopy(buf, pos, dst, o, c);
                pos += c;
                o += c;
                left -= c;
            }
            return true;
        }

        /**
         * The payload of the next record, null at the end of the run.
         */
        byte[] next() {
            final byte[] head = new byte[4];
            if (!readFully(head, 0, 4)) return null;
            final int len = ((head[0] & 0xFF) << 24) | ((head[1] & 0xFF) << 16) | ((head[2] & 0xFF) << 8)
                    | (head[3] & 0xFF);
            final byte[] payload = new byte[len];
            if (len > 0) readFully(payload, 0, len);
            return payload;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                budget.release(buf.length, owner);
            }
        }
    }
}
