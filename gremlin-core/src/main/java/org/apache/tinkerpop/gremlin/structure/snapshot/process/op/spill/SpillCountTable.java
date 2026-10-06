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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Counts the entries of batches by their value, the sum of the bulks per distinct entry, within the quota of its owner.
 * The entries are encoded ({@link KeyCodec}) and counted in a hash table in first-seen order; when the table reaches the
 * quota its entries are written to hash partitions as {@code [type 2][keyLen][key][repLen][rep][count]} records and the
 * table starts again, and {@link #drain} aggregates each partition on its own, splitting it again with another hash if
 * it does not fit. For the entries of a {@code V} or {@code E} lane use {@link SpillFrontier}, which counts by ordinal.
 * <p>
 * The distinct entries come out in {@link #drain} as the objects a standard step would hold (a value, a facade), each
 * removed from the table (and its reservation released) before it is handed to the sink.
 */
public final class SpillCountTable implements AutoCloseable {

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
    private final KeyCodec codec;
    private final String tableOwner;
    private final String buffersOwner;
    private final String partitionOwner;
    private final String readerOwner;
    private final String name;
    private final SpillReserve spillReserve;
    private final Bytes canon = new Bytes();
    private final Bytes rep = new Bytes();

    private Map<KeyBytes, Counter> table = new LinkedHashMap<>();
    private SpillPartitions partitions;

    /**
     * @param owner the prefix of the budget owners of the table, the partition buffers and the partition tables
     */
    public SpillCountTable(final CsrExecutionContext ctx, final String owner) {
        this.ctx = ctx;
        this.codec = new KeyCodec(ctx);
        this.tableOwner = owner + " count table";
        this.buffersOwner = owner + " spill buffers";
        this.partitionOwner = owner + " partition table";
        this.readerOwner = owner + " partition reader";
        this.name = "count-table";
        this.spillReserve = new SpillReserve(ctx, owner + " spill reserve", SpillReserve.PARTITIONS);
        this.spillReserve.hold();
    }

    /**
     * Adds the bulk of entry {@code i} to its count.
     */
    public void add(final Batch b, final int i) {
        canon.reset();
        rep.reset();
        codec.encodeKey(b, i, canon, rep);
        final byte[] key = canon.toArray();
        final KeyBytes k = new KeyBytes(key, rep.size() == 0 ? KeyBytes.EMPTY : rep.toArray());
        final Counter counter = table.get(k);
        if (counter != null) {
            counter.count += b.bulk[i];
            return;
        }
        final long cost = SpillSupport.MAP_ENTRY + key.length + k.rep.length;
        if (!ctx.tryReserveWithinQuota(cost, tableOwner)) {
            if (!table.isEmpty()) {
                spillTable();
            }
            ctx.budget().reserve(cost, tableOwner);
        }
        table.put(k, new Counter(k.rep, b.bulk[i]));
    }

    /**
     * Whether the table was written to scratch, which is when the counts come out of the partitions.
     */
    public boolean isSpilled() {
        return partitions != null;
    }

    private void spillTable() {
        if (partitions == null) {
            spillReserve.handOver();
            partitions = new SpillPartitions(ctx, buffersOwner, name, 0);
        }
        final Bytes record = new Bytes();
        for (final Map.Entry<KeyBytes, Counter> e : table.entrySet()) {
            record.reset();
            record.putByte(COUNT_RECORD);
            record.putBlock(e.getKey().canon);
            record.putBlock(e.getValue().rep);
            record.putLong(e.getValue().count);
            partitions.append(record.toArray());
            ctx.checkInterrupt();
        }
        table.clear();
        ctx.budget().releaseAll(tableOwner);
    }

    /**
     * Hands every distinct entry with its count to the sink and empties the table. The sink may reserve from the
     * budget; what the table held for the entry was released before.
     */
    public void drain(final BiConsumer<Object, Long> sink) {
        if (partitions == null) {
            move(table, tableOwner, sink);
            return;
        }
        spillTable();
        final SpillRun[] runs = partitions.finish();
        partitions = null;
        ctx.budget().releaseAll(buffersOwner);
        for (final SpillRun run : runs) {
            if (run != null) aggregate(run, 0, sink);
        }
    }

    private void move(final Map<KeyBytes, Counter> source, final String stateOwner, final BiConsumer<Object, Long> sink) {
        final Iterator<Map.Entry<KeyBytes, Counter>> it = source.entrySet().iterator();
        while (it.hasNext()) {
            final Map.Entry<KeyBytes, Counter> e = it.next();
            final KeyBytes key = e.getKey();
            final Counter counter = e.getValue();
            it.remove();
            ctx.budget().release(SpillSupport.MAP_ENTRY + key.canon.length + counter.rep.length, stateOwner);
            sink.accept(codec.decodeKey(key.canon, counter.rep), counter.count);
            ctx.checkInterrupt();
        }
    }

    private void aggregate(final SpillRun run, final int depth, final BiConsumer<Object, Long> sink) {
        final Map<KeyBytes, Counter> local = new LinkedHashMap<>();
        boolean overflow = false;
        final SpillRun.Reader reader = new SpillRun.Reader(ctx, run, readerOwner, SpillSupport.bufferBytes(ctx, 4));
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
                if (depth >= SpillSupport.MAX_DEPTH) ctx.budget().reserve(cost, partitionOwner);
                else if (!ctx.tryReserveWithinQuota(cost, partitionOwner)) {
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
                move(local, partitionOwner, sink);
                run.discard(ctx.scratch());
                return;
            }
            local.clear();
        } finally {
            ctx.budget().releaseAll(partitionOwner);
        }
        final SpillRun[] parts = SpillPartitions.split(ctx, buffersOwner, name + depth, run, depth + 1);
        ctx.budget().releaseAll(buffersOwner);
        for (final SpillRun part : parts) {
            if (part != null) aggregate(part, depth + 1, sink);
        }
    }

    /**
     * Discards the counts and the partitions; the table can be filled again.
     */
    public void clear() {
        if (partitions != null) {
            partitions.abort();
            partitions = null;
        }
        table.clear();
        for (final String state : new String[]{tableOwner, buffersOwner, partitionOwner, readerOwner}) {
            ctx.budget().releaseAll(state);
        }
        spillReserve.hold();
    }

    /**
     * Discards everything and releases the spill reserve; the table must not be used afterwards.
     */
    @Override
    public void close() {
        clear();
        spillReserve.release();
    }
}
