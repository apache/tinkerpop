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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The state of {@code groupCount().by(key)} with a size that is bounded by the memory budget, shared by the terminal
 * ({@link SpillGroupCountOperator}) and the side-effect writer ({@link SpillGroupCountWriterOperator}). The result is a
 * {@code HashMap<Object, Long>} like the standard step's.
 * <p>
 * Vertices and edges keyed by themselves count in a {@code long[]} over the ordinals when the budget covers it. Every
 * other key is encoded ({@link KeyCodec}) and counted in a hash table. When the table cannot grow, its entries are
 * written to hash partitions as {@code [type 2][keyLen][key][repLen][rep][count]} records and the table starts again;
 * at the end each partition is aggregated on its own, and split again with another hash if it does not fit. The result
 * map itself is reserved from the budget entry by entry (see {@link ResultMap}).
 * <p>
 * Owners derive from the owning operator's description, which is unique in the traversal.
 */
final class SpillGroupCountState implements AutoCloseable {

    private static final int COUNT_RECORD = 2;

    private static final class Counter {
        final byte[] rep;
        long count;

        Counter(final byte[] rep, final long count) {
            this.rep = rep;
            this.count = count;
        }
    }

    private final CsrExecutionContext ctx;
    private final OperatorStats stats;
    private final String base;
    private final Lane lane;
    private final boolean strict;

    private final KeyCodec codec;
    private final KeyEvaluator keys;
    private final Bytes canon = new Bytes();
    private final Bytes rep = new Bytes();

    private final SpillReserve spillReserve;
    private long[] counts;
    private final Map<KeyBytes, Counter> table = new HashMap<>();
    private SpillPartitions partitions;
    private final ResultMap result;
    private boolean any;

    /**
     * @param base   the owner prefix, the description of the operator
     * @param strict whether a non-productive key throws, as the {@code groupCount('x')} side-effect step does, instead of
     *               dropping the entry
     */
    SpillGroupCountState(final CsrExecutionContext ctx, final OperatorStats stats, final String base,
                         final Keys.Key key, final Lane lane, final boolean strict) {
        this.ctx = ctx;
        this.stats = stats;
        this.base = base;
        this.lane = lane;
        this.strict = strict;
        // headroom for the partition buffers, handed over in spillTable
        spillReserve = new SpillReserve(ctx, owner("spill reserve"), SpillReserve.PARTITIONS);
        spillReserve.hold();
        codec = new KeyCodec(ctx);
        keys = new KeyEvaluator(ctx, codec, key, lane);
        keys.open();
        result = new ResultMap(ctx.budget(), owner("result map"));
        if (keys.isOrdinalIdentity()) {
            final int universe = universe();
            if (ctx.tryReserveWithinQuota(8L * universe, owner("ordinal counts"))) {
                counts = new long[universe];
            }
        }
        stats.annotate("groupCount.mode", counts != null ? "dense" : "hash");
    }

    private String owner(final String state) {
        return base + " " + state;
    }

    private int universe() {
        return lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
    }

    /**
     * Counts the entries of the batch.
     */
    void add(final Batch in) {
        for (int i = 0; i < in.n; i++) count(in, i);
    }

    /**
     * Whether no entry was productive, so that a side-effect writer has nothing to write.
     */
    boolean isEmpty() {
        return !any;
    }

    private void count(final Batch in, final int i) {
        final long bulk = in.bulk[i];
        if (counts != null) {
            any = true;
            counts[in.ord[i]] += bulk;
            return;
        }
        if (!keys.evaluate(in, i)) {
            // TraversalUtil.applyNullable, which the groupCount('x') writer uses, throws
            if (strict) throw new IllegalArgumentException(
                    "The provided traverser does not map to a value: the by() modulator of groupCount");
            return;
        }
        any = true;
        canon.reset();
        rep.reset();
        keys.encode(canon, rep);
        final byte[] key = canon.toArray();
        final KeyBytes k = new KeyBytes(key, rep.size() == 0 ? KeyBytes.EMPTY : rep.toArray());
        final Counter counter = table.get(k);
        if (counter != null) {
            counter.count += bulk;
            return;
        }
        final long cost = SpillSupport.MAP_ENTRY + key.length + k.rep.length;
        if (!ctx.tryReserveWithinQuota(cost, owner("count table"))) {
            if (table.isEmpty()) ctx.budget().reserve(cost, owner("count table"));
            else {
                spillTable();
                ctx.budget().reserve(cost, owner("count table"));
            }
        }
        table.put(k, new Counter(k.rep, bulk));
    }

    /**
     * Builds the result once the input is consumed; the map's budget stays reserved until {@link #reset()} or
     * {@link #close()}, since the consumer keeps the map.
     */
    Map<Object, Object> build() {
        if (counts != null) {
            fromCounts();
        } else if (partitions == null) {
            fromTable();
        } else {
            spillTable();
            final SpillRun[] runs = partitions.finish();
            partitions = null;
            ctx.budget().releaseAll(owner("spill buffers"));
            for (final SpillRun run : runs) {
                if (run != null) aggregate(run, 0);
            }
        }
        return result.handOff();
    }

    private void spillTable() {
        if (partitions == null) {
            spillReserve.handOver();
            partitions = new SpillPartitions(ctx, owner("spill buffers"), base + "-groupCount", 0);
            stats.annotate("groupCount.spilled", "true");
        }
        final Bytes record = new Bytes();
        for (final Map.Entry<KeyBytes, Counter> e : table.entrySet()) {
            record.reset();
            countRecord(record, e.getKey().canon, e.getValue().rep, e.getValue().count);
            partitions.append(record.toArray());
            ctx.checkInterrupt();
        }
        table.clear();
        ctx.budget().releaseAll(owner("count table"));
    }

    private static void countRecord(final Bytes record, final byte[] key, final byte[] rep, final long count) {
        record.putByte(COUNT_RECORD);
        record.putBlock(key);
        record.putBlock(rep);
        record.putLong(count);
    }

    private void fromCounts() {
        for (int ordinal = 0; ordinal < counts.length; ordinal++) {
            if (counts[ordinal] == 0) continue;
            final Object key = lane == Lane.V ? ctx.graph().vertexAt(ordinal) : ctx.graph().edgeAt(ordinal);
            result.put(key, counts[ordinal]);
            if ((ordinal & 0xFFFF) == 0) ctx.checkInterrupt();
        }
        counts = null;
        ctx.budget().releaseAll(owner("ordinal counts"));
    }

    private void fromTable() {
        moveTable(table, owner("count table"));
        ctx.budget().releaseAll(owner("count table"));
    }

    /**
     * Moves the entries to the result, releasing each table entry before reserving its result entry.
     */
    private void moveTable(final Map<KeyBytes, Counter> source, final String stateOwner) {
        final Iterator<Map.Entry<KeyBytes, Counter>> it = source.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<KeyBytes, Counter> e = it.next();
            final KeyBytes key = e.getKey();
            final Counter counter = e.getValue();
            // release the table entry before reserving the result entry, so a full table can still drain
            it.remove();
            ctx.budget().release(SpillSupport.MAP_ENTRY + key.canon.length + counter.rep.length, stateOwner);
            result.put(codec.decodeKey(key.canon, counter.rep), counter.count);
            ctx.checkInterrupt();
        }
    }

    private void aggregate(final SpillRun run, final int depth) {
        final String tableOwner = owner("partition table");
        final Map<KeyBytes, Counter> local = new HashMap<>();
        boolean overflow = false;
        final SpillRun.Reader reader = new SpillRun.Reader(ctx, run, owner("partition reader"),
                SpillSupport.bufferBytes(ctx, 4));
        try {
            byte[] payload;
            while ((payload = reader.next()) != null) {
                final ByteSource source = new ByteSource(payload, 1);
                final byte[] key = source.getBlock();
                final byte[] repBytes = source.getBlock();
                final long count = source.getLong();
                final KeyBytes k = new KeyBytes(key, repBytes);
                final Counter counter = local.get(k);
                if (counter != null) {
                    counter.count += count;
                    continue;
                }
                final long cost = SpillSupport.MAP_ENTRY + key.length + repBytes.length;
                if (depth >= SpillSupport.MAX_DEPTH) ctx.budget().reserve(cost, tableOwner);
                else if (!ctx.tryReserveWithinQuota(cost, tableOwner)) {
                    overflow = true;
                    break;
                }
                local.put(k, new Counter(repBytes, count));
                ctx.checkInterrupt();
            }
        } finally {
            reader.close();
        }
        try {
            if (!overflow) {
                moveTable(local, tableOwner);
                run.discard(ctx.scratch());
                return;
            }
            local.clear();
        } finally {
            ctx.budget().releaseAll(tableOwner);
        }
        final SpillRun[] parts = SpillPartitions.split(ctx, owner("spill buffers"), base + "-groupCount" + depth,
                run, depth + 1);
        ctx.budget().releaseAll(owner("spill buffers"));
        for (final SpillRun part : parts) {
            if (part != null) aggregate(part, depth + 1);
        }
    }

    private void clearState() {
        if (partitions != null) {
            partitions.abort();
            partitions = null;
        }
        spillReserve.release();
        table.clear();
        result.clear();
        for (final String state : new String[]{"count table", "spill buffers", "partition table", "partition reader"}) {
            ctx.budget().releaseAll(owner(state));
        }
    }

    /**
     * Back to the state after construction: nothing counted, the result released.
     */
    void reset() {
        clearState();
        spillReserve.hold();
        any = false;
        if (counts != null) Arrays.fill(counts, 0);
        else if (ctx.budget().reserved(owner("ordinal counts")) == 0 && keys.isOrdinalIdentity()) {
            // the counts were handed to the result; take them again if the budget still covers them
            final int universe = universe();
            if (ctx.tryReserveWithinQuota(8L * universe, owner("ordinal counts"))) counts = new long[universe];
        }
        keys.reset();
    }

    @Override
    public void close() {
        try {
            clearState();
            ctx.budget().releaseAll(owner("ordinal counts"));
            counts = null;
        } finally {
            keys.close();
        }
    }
}
