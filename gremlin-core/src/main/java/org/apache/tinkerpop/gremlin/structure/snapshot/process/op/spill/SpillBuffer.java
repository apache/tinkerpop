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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;

import java.util.ArrayList;
import java.util.List;

/**
 * Keeps a stream of batch entries, with their bulk, in memory up to the quota of its owner and in a scratch run beyond
 * it, and replays them any number of times in arrival order. It is the buffer behind operators that need their whole
 * input before they can emit (the stateful branches of {@code union} and {@code choose}, {@code aggregate()}).
 * <p>
 * Usage is {@link #add} for every entry, then {@link #finish()} (implied by {@link #replay()}), then any number of
 * {@link #replay()}s; {@link #clear()} empties the buffer for reuse and {@link #close()} gives everything back for good.
 * The memory batches are reserved per batch within the quota ({@link CsrExecutionContext#tryReserveWithinQuota}),
 * except for the first one, which the buffer needs to work at all. When the quota is exceeded the memory batches move to
 * a run and the buffer writes the following entries to it directly. Entries of the {@code SCALAR} lane cannot be
 * encoded, so a buffer of that lane never spills and draws from the budget without a quota.
 * <p>
 * At most one {@link #replay()} is active: a new one ends the previous.
 */
public final class SpillBuffer implements AutoCloseable {

    private final CsrExecutionContext ctx;
    private final String owner;
    private final Lane lane;
    private final boolean recordSource;
    private final int capacity;
    private final SpillReserve spillReserve;
    private final EntryCodec entries;
    private final Bytes encoded = new Bytes();

    private final List<Batch> batches = new ArrayList<>();
    private Batch tail;
    private long reserved;
    private long count;

    private SpillRun.Writer writer;
    private SpillRun run;
    private boolean finished;
    private Replay active;

    /**
     * @param owner        the budget owner of the memory batches and the buffers, used for nothing else
     * @param recordSource whether the entries of an {@code E} lane carry their source vertex
     */
    public SpillBuffer(final CsrExecutionContext ctx, final String owner, final Lane lane, final boolean recordSource) {
        this.ctx = ctx;
        this.owner = owner;
        this.lane = lane;
        this.recordSource = recordSource;
        // the memory batches are reserved whole, so a small quota shrinks them
        this.capacity = Batch.capacityWithin(lane, recordSource, ctx.batchSize(), ctx.quota() / 4);
        this.spillReserve = new SpillReserve(ctx, owner + " spill reserve", SpillReserve.RUN);
        this.entries = lane == Lane.SCALAR ? null : new EntryCodec(new KeyCodec(ctx), lane, recordSource);
        this.spillReserve.hold();
    }

    /**
     * Adds entry {@code i} of the batch with its bulk.
     */
    public void add(final Batch source, final int i) {
        add(source, i, source.bulk[i]);
    }

    public void add(final Batch source, final int i, final long bulk) {
        if (finished) throw new IllegalStateException("The buffer is finished; clear() it before adding");
        if (source.lane != lane) throw new IllegalArgumentException("Batch lane " + source.lane + " is not " + lane);
        count++;
        if (writer == null) {
            if (tail != null && !tail.isFull()) {
                tail.copyEntry(source, i, bulk);
                return;
            }
            final Batch fresh = new Batch(lane, capacity, recordSource);
            final long bytes = fresh.estimatedBytes();
            if (batches.isEmpty() || entries == null) {
                ctx.budget().reserve(bytes, owner);
            } else if (!ctx.tryReserveWithinQuota(bytes, owner)) {
                spill();
                write(source, i, bulk);
                return;
            }
            reserved += bytes;
            batches.add(fresh);
            tail = fresh;
            tail.copyEntry(source, i, bulk);
            return;
        }
        write(source, i, bulk);
    }

    private void write(final Batch source, final int i, final long bulk) {
        encoded.reset();
        entries.encode(source, i, bulk, encoded);
        writer.writeRecord(encoded.toArray());
        ctx.checkInterrupt();
    }

    /**
     * Moves the memory batches to a run, which gives their reservation back.
     */
    private void spill() {
        spillReserve.handOver();
        writer = new SpillRun.Writer(ctx, owner, "spill-buffer", SpillSupport.bufferBytes(ctx, 4));
        try {
            for (final Batch b : batches) {
                for (int i = 0; i < b.n; i++) write(b, i, b.bulk[i]);
            }
        } catch (RuntimeException e) {
            writer.abort();
            writer = null;
            throw e;
        }
        dropMemory();
    }

    private void dropMemory() {
        batches.clear();
        tail = null;
        if (reserved > 0) ctx.budget().release(reserved, owner);
        reserved = 0;
    }

    /**
     * Ends the add phase. Idempotent.
     */
    public void finish() {
        if (finished) return;
        finished = true;
        if (writer != null) {
            run = writer.finish();
            writer = null;
        }
    }

    /**
     * Whether any entry went to scratch since the last {@link #clear()}.
     */
    public boolean isSpilled() {
        return writer != null || run != null;
    }

    /**
     * The number of entries added since the last {@link #clear()}.
     */
    public long entries() {
        return count;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    /**
     * A supplier of all entries from the start, in arrival order, in batches of the output's capacity. It can be taken
     * again and again; taking one ends the previous. The supplier releases its read buffer when it is exhausted or
     * closed, and {@link #clear()} and {@link #close()} close it.
     */
    public Replay replay() {
        finish();
        closeReplay();
        active = new Replay();
        return active;
    }

    private void closeReplay() {
        if (active != null) {
            active.close();
            active = null;
        }
    }

    /**
     * Discards the entries, in memory and on scratch, and gives their reservation back; the buffer can be filled again.
     */
    public void clear() {
        closeReplay();
        if (writer != null) {
            writer.abort();
            writer = null;
        }
        if (run != null) {
            run.discard(ctx.scratch());
            run = null;
        }
        dropMemory();
        count = 0;
        finished = false;
        encoded.reset();
        spillReserve.hold();
    }

    /**
     * Discards everything and releases the spill reserve; the buffer must not be used afterwards.
     */
    @Override
    public void close() {
        clear();
        spillReserve.release();
    }

    /**
     * One pass over the entries of the buffer.
     */
    public final class Replay implements BatchSupplier, AutoCloseable {

        private int batch;
        private int position;
        private SpillRun.Reader reader;
        private boolean done;

        private Replay() {
            if (run != null) {
                reader = new SpillRun.Reader(ctx, run, owner, SpillSupport.bufferBytes(ctx, 4));
            }
        }

        @Override
        public boolean next(final Batch out) {
            out.clear();
            if (done) return false;
            if (reader != null) {
                byte[] payload;
                while (!out.isFull() && (payload = reader.next()) != null) {
                    entries.decode(new ByteSource(payload), out);
                }
            } else {
                while (!out.isFull() && batch < batches.size()) {
                    final Batch source = batches.get(batch);
                    if (position < source.n) {
                        out.copyEntry(source, position++);
                    } else {
                        batch++;
                        position = 0;
                    }
                }
            }
            if (out.n == 0) {
                close();
                return false;
            }
            ctx.checkInterrupt();
            return true;
        }

        @Override
        public void close() {
            done = true;
            if (reader != null) {
                reader.close();
                reader = null;
            }
        }
    }
}
