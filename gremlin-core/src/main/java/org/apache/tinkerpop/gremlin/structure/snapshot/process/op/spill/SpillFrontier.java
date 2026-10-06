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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.DenseFrontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Frontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link Frontier} that stays within the memory budget. It has three states and moves between them:
 * <ul>
 * <li>dense: a {@link DenseFrontier}, when the expected number of entries makes {@code long[universe]} worthwhile and the
 * budget covers it;</li>
 * <li>sparse: ordinals and bulks in reserved arrays that grow while the budget allows, up to a cap;</li>
 * <li>spilled: when the arrays cannot grow they are sorted, merged and written to a scratch run of
 * {@code [ordinal][bulk]} records, and sealing merges the runs into one.</li>
 * </ul>
 * A dense frontier is downgraded to sparse when a level turned out to hold fewer than {@code universe / 64} distinct
 * ordinals (checked at {@link #seal()} and {@link #clear()}), which gives the {@code long[universe]} back to the budget.
 * A sparse frontier is upgraded to dense at {@link #clear()} when the last level was dense enough and the budget covers
 * it. Draining is in ascending ordinal order with equal ordinals merged, like the other frontiers.
 * <p>
 * {@link Frontier#create} cannot route here, since it is a frozen factory; an operator that wants a spillable frontier
 * calls {@link #create}.
 */
public final class SpillFrontier implements Frontier {

    private static final int INITIAL = 256;
    // 12 bytes of ordinal and bulk plus 20 bytes of sortAndMerge temporaries (order, merged ordinals, merged bulks)
    private static final long ENTRY_BYTES = 32L;
    private static final int MAX_BUFFER_ENTRIES = 1 << 20;
    private static final int DRAIN_BUFFER = 16 << 10;

    private final CsrExecutionContext ctx;
    private final MemoryBudget budget;
    private final String owner;
    private final Lane lane;
    private final int universe;
    // headroom for the run writers, see SpillReserve
    private final SpillReserve spillReserve;

    private DenseFrontier dense;
    private int[] ords;
    private long[] bulks;
    private int size;
    private long bufferReserved;
    private boolean sealed = true;
    private int cursor;

    private final List<SpillRun> runs = new ArrayList<>();
    private SpillRun merged;
    private long mergedDistinct;
    private SpillRun.Reader drainReader;
    private long drainReserved;
    private long lastDistinct;
    private boolean released;

    private SpillFrontier(final CsrExecutionContext ctx, final String owner, final Lane lane, final int universe) {
        if (!lane.isElement()) throw new IllegalArgumentException("A frontier holds vertices or edges, not " + lane);
        this.ctx = ctx;
        this.budget = ctx.budget();
        this.owner = owner;
        this.lane = lane;
        this.universe = universe;
        this.spillReserve = new SpillReserve(ctx, owner + " spill reserve", SpillReserve.RUN);
    }

    /**
     * Creates a frontier that is dense when {@code expectedEntries * 64 >= universe} and the budget covers it, sparse
     * otherwise.
     *
     * @throws org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrMemoryBudgetException if not even the
     *                                                                                               initial sparse arrays fit
     */
    public static SpillFrontier create(final CsrExecutionContext ctx, final String owner, final Lane lane,
                                       final int universe, final long expectedEntries) {
        final SpillFrontier f = new SpillFrontier(ctx, owner, lane, universe);
        f.spillReserve.hold();
        if (expectedEntries * DENSE_RATIO >= universe && DenseFrontier.bytesFor(universe) <= ctx.budget().available()) {
            f.dense = new DenseFrontier(ctx.budget(), owner, lane, universe);
        } else {
            f.allocateBuffer(INITIAL);
        }
        return f;
    }

    @Override
    public Lane lane() {
        return lane;
    }

    @Override
    public boolean isDense() {
        return dense != null;
    }

    /**
     * Whether entries were written to scratch since the last {@link #clear()}.
     */
    public boolean isSpilled() {
        return !runs.isEmpty() || merged != null;
    }

    // ---------------------------------------------------------------- building

    @Override
    public void add(final int ordinal, final long bulk) {
        if (dense != null) {
            dense.add(ordinal, bulk);
            return;
        }
        if (merged != null) {
            closeDrain();
            runs.add(merged);
            merged = null;
        }
        if (size == ords.length) grow();
        ords[size] = ordinal;
        bulks[size++] = bulk;
        sealed = false;
    }

    private void allocateBuffer(final int capacity) {
        budget.reserve(ENTRY_BYTES * capacity, owner);
        bufferReserved = ENTRY_BYTES * capacity;
        ords = new int[capacity];
        bulks = new long[capacity];
        size = 0;
    }

    private void grow() {
        final int capacity = (int) Math.min(MAX_BUFFER_ENTRIES, 2L * ords.length);
        final long extra = ENTRY_BYTES * (capacity - ords.length);
        if (capacity > ords.length && budget.tryReserve(extra, owner)) {
            bufferReserved += extra;
            ords = Arrays.copyOf(ords, capacity);
            bulks = Arrays.copyOf(bulks, capacity);
            return;
        }
        spillBuffer();
    }

    /**
     * Sorts and merges the buffer and writes it as a run.
     */
    private void spillBuffer() {
        sortAndMerge();
        spillReserve.handOver();
        final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner, "frontier-run" + runs.size(),
                SpillSupport.bufferBytes(ctx, 4));
        try {
            final Bytes record = new Bytes();
            for (int i = 0; i < size; i++) {
                record.reset();
                record.putInt(ords[i]);
                record.putLong(bulks[i]);
                writer.writeRecord(record.toArray());
            }
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
        runs.add(writer.finish());
        spillReserve.retake();
        size = 0;
        sealed = true;
    }

    private void sortAndMerge() {
        final long[] order = new long[size];
        for (int i = 0; i < size; i++) order[i] = ((long) ords[i] << 32) | i;
        Arrays.sort(order);
        final int[] mergedOrds = new int[size];
        final long[] mergedBulks = new long[size];
        int m = 0;
        for (int i = 0; i < size; i++) {
            final int ordinal = (int) (order[i] >>> 32);
            final long bulk = bulks[(int) order[i]];
            if (m > 0 && mergedOrds[m - 1] == ordinal) {
                mergedBulks[m - 1] += bulk;
            } else {
                mergedOrds[m] = ordinal;
                mergedBulks[m++] = bulk;
            }
        }
        System.arraycopy(mergedOrds, 0, ords, 0, m);
        System.arraycopy(mergedBulks, 0, bulks, 0, m);
        size = m;
    }

    @Override
    public void seal() {
        if (dense != null) {
            if (dense.distinct() * DENSE_RATIO < universe) downgrade();
            if (dense != null) return;
        }
        if (sealed) return;
        if (runs.isEmpty()) {
            sortAndMerge();
            sealed = true;
            cursor = 0;
            return;
        }
        if (size > 0) spillBuffer();
        mergeRuns();
        sealed = true;
        cursor = 0;
    }

    /**
     * Merges the runs into one, counting the distinct ordinals, and keeps that as the sealed state.
     */
    private void mergeRuns() {
        spillReserve.handOver();
        final List<SpillRun> reduced = Merger.reduce(ctx, runs, owner, "frontier", SpillFrontier::ordinalOf,
                Integer::compare);
        runs.clear();
        final int[] plan = SpillSupport.mergePlan(ctx);
        final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner, "frontier-merged", plan[1]);
        long distinct = 0;
        try (Merger<Integer> merger = new Merger<>(ctx, reduced, owner, plan[1], SpillFrontier::ordinalOf,
                Integer::compare)) {
            int pendingOrdinal = -1;
            long pendingBulk = 0;
            boolean pending = false;
            final Bytes record = new Bytes();
            while (merger.advance()) {
                final ByteSource source = new ByteSource(merger.payload());
                final int ordinal = source.getInt();
                final long bulk = source.getLong();
                if (pending && ordinal == pendingOrdinal) {
                    pendingBulk += bulk;
                    continue;
                }
                if (pending) {
                    record.reset();
                    record.putInt(pendingOrdinal);
                    record.putLong(pendingBulk);
                    writer.writeRecord(record.toArray());
                    distinct++;
                }
                pending = true;
                pendingOrdinal = ordinal;
                pendingBulk = bulk;
                ctx.checkInterrupt();
            }
            if (pending) {
                record.reset();
                record.putInt(pendingOrdinal);
                record.putLong(pendingBulk);
                writer.writeRecord(record.toArray());
                distinct++;
            }
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
        merged = writer.finish();
        mergedDistinct = distinct;
        for (final SpillRun r : reduced) r.discard(ctx.scratch());
        spillReserve.retake();
        size = 0;
    }

    private static Integer ordinalOf(final byte[] payload) {
        return new ByteSource(payload).getInt();
    }

    private void downgrade() {
        final DenseFrontier d = dense;
        final int capacity = (int) Math.max(INITIAL, Math.min(MAX_BUFFER_ENTRIES, d.distinct()));
        if (!budget.tryReserve(ENTRY_BYTES * capacity, owner)) return;
        bufferReserved = ENTRY_BYTES * capacity;
        ords = new int[capacity];
        bulks = new long[capacity];
        size = 0;
        dense = null;
        d.rewind();
        final Batch batch = new Batch(lane, 1024);
        while (d.drain(batch) > 0) {
            for (int i = 0; i < batch.n; i++) add(batch.ord[i], batch.bulk[i]);
            batch.clear();
        }
        d.release();
        sealed = runs.isEmpty();
        cursor = 0;
    }

    // ---------------------------------------------------------------- reading

    @Override
    public long distinct() {
        if (dense != null) return dense.distinct();
        if (merged != null) return mergedDistinct;
        long n = size;
        for (final SpillRun run : runs) n += run.records();
        return n;
    }

    @Override
    public int drain(final Batch out) {
        if (out.lane != lane) throw new IllegalArgumentException("Batch lane " + out.lane + " is not " + lane);
        seal();
        if (dense != null) return dense.drain(out);
        int appended = 0;
        if (merged == null) {
            while (!out.isFull() && cursor < size) {
                out.ord[out.n] = ords[cursor];
                out.bulk[out.n++] = bulks[cursor++];
                appended++;
            }
            return appended;
        }
        if (drainReader == null) {
            final int buffer = SpillSupport.bufferBytes(ctx, 4);
            drainReader = new SpillRun.Reader(ctx, merged, owner, buffer);
            drainReserved = buffer;
        }
        while (!out.isFull()) {
            final byte[] payload = drainReader.next();
            if (payload == null) break;
            final ByteSource source = new ByteSource(payload);
            out.ord[out.n] = source.getInt();
            out.bulk[out.n++] = source.getLong();
            appended++;
        }
        return appended;
    }

    private void closeDrain() {
        if (drainReader != null) {
            drainReader.close();
            drainReader = null;
            drainReserved = 0;
        }
    }

    @Override
    public void rewind() {
        if (dense != null) {
            dense.rewind();
            return;
        }
        cursor = 0;
        closeDrain();
    }

    @Override
    public void clear() {
        lastDistinct = distinct();
        closeDrain();
        discardRuns();
        if (dense != null) {
            if (lastDistinct * DENSE_RATIO < universe) {
                dense.release();
                dense = null;
                allocateBuffer(INITIAL);
            } else {
                dense.clear();
            }
            return;
        }
        size = 0;
        cursor = 0;
        sealed = true;
        if (lastDistinct * DENSE_RATIO >= universe && DenseFrontier.bytesFor(universe) <= budget.available()) {
            budget.release(bufferReserved, owner);
            bufferReserved = 0;
            ords = null;
            bulks = null;
            dense = new DenseFrontier(budget, owner, lane, universe);
        }
    }

    private void discardRuns() {
        for (final SpillRun run : runs) run.discard(ctx.scratch());
        runs.clear();
        if (merged != null) {
            merged.discard(ctx.scratch());
            merged = null;
        }
        mergedDistinct = 0;
    }

    @Override
    public long reservedBytes() {
        return (dense != null ? dense.reservedBytes() : 0) + bufferReserved + drainReserved;
    }

    @Override
    public void release() {
        if (released) return;
        released = true;
        closeDrain();
        discardRuns();
        if (dense != null) {
            dense.release();
            dense = null;
        }
        budget.release(bufferReserved, owner);
        bufferReserved = 0;
        spillReserve.release();
        ords = null;
        bulks = null;
        size = 0;
    }
}
