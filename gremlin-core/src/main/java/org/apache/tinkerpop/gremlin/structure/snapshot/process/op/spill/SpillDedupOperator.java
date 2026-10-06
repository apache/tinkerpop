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

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@code dedup()} and {@code dedup().by(key)} with state that is bounded by the memory budget.
 * <p>
 * Vertices and edges keyed by themselves use a bitset over the ordinals when the budget covers it. Every other key is
 * encoded ({@link KeyCodec}) and kept in a hash set, and entries are emitted as they arrive. When the set cannot grow,
 * its keys are written to hash partitions as <em>seen</em> markers, and from then on every productive entry is written
 * to its partition with its arrival sequence instead of being emitted. At the end of the input each partition is
 * replayed in arrival order against its markers: an entry whose key has not been seen survives. A partition whose set
 * does not fit is split again with another hash, up to {@link SpillSupport#MAX_DEPTH} times. The survivors of all
 * partitions are merged by sequence, so the output keeps arrival order, and have bulk 1 like the in-memory operator.
 * <p>
 * Record formats: {@code [type 0][keyLen][key]} marker, {@code [type 1][keyLen][key][seq][entry]} entry; survivor
 * record {@code [seq][entry]}.
 */
final class SpillDedupOperator extends AbstractCsrOperator {

    private static final int MARKER = 0;
    private static final int ENTRY = 1;

    private enum Phase { STREAM, DRAIN, DONE }

    private final Ops.Dedup node;
    private final Lane lane;

    private KeyCodec codec;
    private KeyEvaluator keys;
    private EntryCodec entries;
    private Batch in;
    private int pos;
    private final Bytes canon = new Bytes();
    private final Bytes rep = new Bytes();
    private final Bytes record = new Bytes();

    private SpillReserve spillReserve;
    private long[] bits;
    private Set<KeyBytes> seen;
    private long seenBytes;
    private SpillPartitions partitions;
    private long sequence;
    private final List<SpillRun> survivors = new ArrayList<>();
    private Merger<Long> merger;
    private Phase phase = Phase.STREAM;
    private int spilledRecords;

    SpillDedupOperator(final Ops.Dedup node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
        this.lane = spec.inputLane();
    }

    @Override
    protected void doOpen() {
        // headroom for the partition buffers, handed over in startSpill
        spillReserve = new SpillReserve(ctx, owner("spill reserve"), SpillReserve.PARTITIONS);
        spillReserve.hold();
        codec = new KeyCodec(ctx);
        keys = new KeyEvaluator(ctx, codec, node.key(), lane);
        keys.open();
        entries = new EntryCodec(codec, lane, spec.inputRecordsSource());
        in = spec.newInputBatch(ctx.batchSize());
        seen = new HashSet<>();
        if (keys.isOrdinalIdentity()) {
            final int universe = lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
            final long words = (universe + 63L) >>> 6;
            if (ctx.budget().tryReserve(8 * words, owner("ordinal bitset"))) {
                bits = new long[(int) words];
            }
        }
        stats().annotate("dedup.mode", bits != null ? "bitset" : "hash");
    }

    @Override
    protected boolean produce(final Batch out) {
        if (phase == Phase.STREAM) {
            while (!out.isFull()) {
                if (pos >= in.n) {
                    if (!pull(in)) {
                        finishStream();
                        break;
                    }
                    pos = 0;
                }
                entry(out);
            }
        }
        if (phase == Phase.DRAIN) {
            while (!out.isFull() && merger.advance()) {
                final ByteSource source = new ByteSource(merger.payload(), 8);
                entries.decode(source, out);
            }
            if (!out.isFull()) {
                merger.close();
                merger = null;
                phase = Phase.DONE;
            }
        }
        return phase != Phase.DONE || out.n > 0;
    }

    private void entry(final Batch out) {
        final int i = pos++;
        if (bits != null) {
            final int ordinal = in.ord[i];
            final long bit = 1L << (ordinal & 63);
            if ((bits[ordinal >>> 6] & bit) == 0) {
                bits[ordinal >>> 6] |= bit;
                out.copyEntry(in, i, 1);
            }
            return;
        }
        if (!keys.evaluate(in, i)) return;
        canon.reset();
        rep.reset();
        keys.encode(canon, rep);
        if (partitions == null) {
            final byte[] key = canon.toArray();
            final KeyBytes k = new KeyBytes(key, KeyBytes.EMPTY);
            if (seen.contains(k)) return;
            final long cost = SpillSupport.SET_ENTRY + key.length;
            if (ctx.tryReserveWithinQuota(cost, owner("seen keys"))) {
                seen.add(k);
                seenBytes += cost;
                out.copyEntry(in, i, 1);
                return;
            }
            startSpill();
        }
        spill(i);
    }

    private void startSpill() {
        spillReserve.handOver();
        partitions = new SpillPartitions(ctx, owner("spill buffers"), describe() + "-dedup", 0);
        final Bytes marker = new Bytes();
        for (final KeyBytes k : seen) {
            marker.reset();
            marker.putByte(MARKER);
            marker.putBlock(k.canon);
            partitions.append(marker.toArray());
            ctx.checkInterrupt();
        }
        seen.clear();
        ctx.budget().releaseAll(owner("seen keys"));
        seenBytes = 0;
        stats().annotate("dedup.spilled", "true");
    }

    private void spill(final int i) {
        record.reset();
        record.putByte(ENTRY);
        record.putInt(canon.size());
        record.putBytes(canon);
        record.putLong(sequence++);
        entries.encode(in, i, 1, record);
        partitions.append(record.toArray());
        spilledRecords++;
    }

    private void finishStream() {
        if (partitions == null) {
            phase = Phase.DONE;
            return;
        }
        final SpillRun[] runs = partitions.finish();
        partitions = null;
        ctx.budget().releaseAll(owner("spill buffers"));
        for (final SpillRun run : runs) {
            if (run != null) replay(run, 0);
        }
        final List<SpillRun> reduced = Merger.reduce(ctx, survivors, owner("merge buffers"), describe() + "-dedup",
                SpillDedupOperator::sequenceOf, Long::compare);
        survivors.clear();
        survivors.addAll(reduced);
        final int[] plan = SpillSupport.mergePlan(ctx);
        merger = new Merger<>(ctx, survivors, owner("merge buffers"), plan[1], SpillDedupOperator::sequenceOf,
                Long::compare);
        stats().annotate("dedup.spilledRecords", spilledRecords);
        phase = Phase.DRAIN;
    }

    private static Long sequenceOf(final byte[] payload) {
        return new ByteSource(payload).getLong();
    }

    /**
     * Replays a partition against its markers. If the set of keys does not fit, splits the partition and replays the
     * parts.
     */
    private void replay(final SpillRun run, final int depth) {
        final String setOwner = owner("partition keys");
        final SpillRun.Writer survivorWriter = new SpillRun.Writer(ctx, owner("survivor buffer"),
                describe() + "-survivors", SpillSupport.bufferBytes(ctx, 4));
        final Set<KeyBytes> set = new HashSet<>();
        boolean overflow = false;
        final SpillRun.Reader reader = new SpillRun.Reader(ctx, run, owner("partition reader"),
                SpillSupport.bufferBytes(ctx, 4));
        try {
            byte[] payload;
            while (!overflow && (payload = reader.next()) != null) {
                final ByteSource source = new ByteSource(payload);
                final int type = source.getByte();
                final byte[] key = source.getBlock();
                final KeyBytes k = new KeyBytes(key, KeyBytes.EMPTY);
                if (set.contains(k)) continue;
                final long cost = SpillSupport.SET_ENTRY + key.length;
                if (depth >= SpillSupport.MAX_DEPTH) ctx.budget().reserve(cost, setOwner);
                else if (!ctx.tryReserveWithinQuota(cost, setOwner)) {
                    overflow = true;
                    break;
                }
                set.add(k);
                if (type == ENTRY) {
                    // the record after the key is [seq][entry]
                    survivorWriter.writeRecord(payload, 1 + 4 + key.length, payload.length - 5 - key.length);
                }
                ctx.checkInterrupt();
            }
        } catch (RuntimeException e) {
            survivorWriter.abort();
            throw e;
        } finally {
            reader.close();
            ctx.budget().releaseAll(setOwner);
        }
        if (!overflow) {
            survivors.add(survivorWriter.finish());
            run.discard(ctx.scratch());
            return;
        }
        survivorWriter.abort();
        final SpillRun[] parts = SpillPartitions.split(ctx, owner("spill buffers"), describe() + "-dedup" + depth, run,
                depth + 1);
        ctx.budget().releaseAll(owner("spill buffers"));
        for (final SpillRun part : parts) {
            if (part != null) replay(part, depth + 1);
        }
    }

    private void clearState() {
        if (merger != null) {
            merger.close();
            merger = null;
        }
        if (partitions != null) {
            partitions.abort();
            partitions = null;
        }
        for (final SpillRun run : survivors) run.discard(ctx.scratch());
        survivors.clear();
        if (seen != null) seen.clear();
        seenBytes = 0;
        spillReserve.release();
        for (final String state : new String[]{"seen keys", "spill buffers", "merge buffers", "survivor buffer",
                "partition reader", "partition keys"}) {
            ctx.budget().releaseAll(owner(state));
        }
        sequence = 0;
        spilledRecords = 0;
        pos = 0;
        if (in != null) in.clear();
    }

    @Override
    protected void doReset() {
        clearState();
        spillReserve.hold();
        if (bits != null) Arrays.fill(bits, 0);
        keys.reset();
        phase = Phase.STREAM;
    }

    @Override
    protected void doClose() {
        try {
            clearState();
            ctx.budget().releaseAll(owner("ordinal bitset"));
            bits = null;
        } finally {
            if (keys != null) keys.close();
        }
    }
}
