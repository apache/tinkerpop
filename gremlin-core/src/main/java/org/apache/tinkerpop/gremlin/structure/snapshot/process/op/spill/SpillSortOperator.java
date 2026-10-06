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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Terminals;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * {@code order().by(...)} without a limit as an external merge sort. Records are the entry (with its bulk), the sort
 * keys and the arrival sequence. Keys compare with {@link Order#compare}, which is the {@code ORDERABILITY} comparison of
 * the standard step, and ties go to the earlier arrival, so the sort is stable like {@code TraverserSet.sort}. An entry
 * with a non-productive key is dropped, as the standard step does.
 * <p>
 * Records are buffered in memory and reserved from the budget. When the buffer cannot grow it is sorted and written as
 * a run: {@code [seq][entryLen][entry][keyCount]} and then each key as a length-prefixed {@link KeyCodec} value. At the
 * end, runs are merged in passes of as many runs as the budget has read buffers for, and the last pass streams into the
 * output batches. If nothing was spilled the sorted buffer is emitted directly.
 */
final class SpillSortOperator extends AbstractCsrOperator {

    private static final class Rec {
        final long seq;
        final byte[] entry;
        final Object[] keys;
        final long bytes;

        Rec(final long seq, final byte[] entry, final Object[] keys, final long bytes) {
            this.seq = seq;
            this.entry = entry;
            this.keys = keys;
            this.bytes = bytes;
        }
    }

    private enum Phase { LOAD, MEMORY, MERGE, DONE }

    private final Terminals.Sort node;
    private final Lane lane;
    private final Order[] orders;
    private final Comparator<Rec> comparator;

    private KeyCodec codec;
    private final List<KeyEvaluator> keyEvaluators = new ArrayList<>();
    private EntryCodec entries;
    private Batch in;
    private final Bytes entry = new Bytes();

    private SpillReserve spillReserve;
    private final List<Rec> buffer = new ArrayList<>();
    private long sequence;
    private final List<SpillRun> runs = new ArrayList<>();
    private int next;
    private Merger<Rec> merger;
    private Phase phase = Phase.LOAD;

    SpillSortOperator(final Terminals.Sort node, final OperatorSpec spec) {
        super(spec);
        this.node = node;
        this.lane = spec.inputLane();
        this.orders = node.orders().toArray(new Order[0]);
        this.comparator = (a, b) -> {
            for (int k = 0; k < orders.length; k++) {
                final int c = orders[k].compare(a.keys[k], b.keys[k]);
                if (c != 0) return c;
            }
            return Long.compare(a.seq, b.seq);
        };
    }

    @Override
    protected void doOpen() {
        // headroom for the run writer, handed over in spillBuffer and taken again after each run
        spillReserve = new SpillReserve(ctx, owner("spill reserve"), SpillReserve.RUN);
        spillReserve.hold();
        codec = new KeyCodec(ctx);
        for (final Keys.Key key : node.keys()) {
            final KeyEvaluator evaluator = new KeyEvaluator(ctx, codec, key, lane);
            keyEvaluators.add(evaluator);
            evaluator.open();
        }
        entries = new EntryCodec(codec, lane, spec.inputRecordsSource());
        in = spec.newInputBatch(ctx.batchSize());
    }

    @Override
    protected boolean produce(final Batch out) {
        if (phase == Phase.LOAD) load();
        if (phase == Phase.MEMORY) {
            while (!out.isFull() && next < buffer.size()) {
                final Rec r = buffer.get(next);
                buffer.set(next++, null);
                entries.decode(new ByteSource(r.entry), out);
            }
            if (next >= buffer.size()) {
                buffer.clear();
                ctx.budget().releaseAll(owner("sort buffer"));
                phase = Phase.DONE;
            }
        } else if (phase == Phase.MERGE) {
            while (!out.isFull() && merger.advance()) {
                entries.decode(new ByteSource(merger.value().entry), out);
            }
            if (!out.isFull()) {
                merger.close();
                merger = null;
                phase = Phase.DONE;
            }
        }
        return phase != Phase.DONE || out.n > 0;
    }

    private void load() {
        final Object[] scratch = new Object[keyEvaluators.size()];
        while (pull(in)) {
            for (int i = 0; i < in.n; i++) {
                boolean productive = true;
                for (int k = 0; k < scratch.length && productive; k++) {
                    final KeyEvaluator evaluator = keyEvaluators.get(k);
                    productive = evaluator.evaluate(in, i);
                    if (productive) scratch[k] = evaluator.object();
                }
                if (!productive) continue;
                entry.reset();
                entries.encode(in, i, in.bulk[i], entry);
                final byte[] bytes = entry.toArray();
                long size = SpillSupport.RECORD + bytes.length + 8L * scratch.length;
                for (final Object key : scratch) size += SpillSupport.estimate(key);
                if (!ctx.tryReserveWithinQuota(size, owner("sort buffer"))) {
                    if (!buffer.isEmpty()) spillBuffer();
                    ctx.budget().reserve(size, owner("sort buffer"));
                }
                buffer.add(new Rec(sequence++, bytes, scratch.clone(), size));
            }
            ctx.checkInterrupt();
        }
        if (runs.isEmpty()) {
            buffer.sort(comparator);
            next = 0;
            phase = Phase.MEMORY;
            stats().annotate("sort.spilled", "false");
            return;
        }
        if (!buffer.isEmpty()) spillBuffer();
        final List<SpillRun> reduced = Merger.reduce(ctx, runs, owner("merge buffers"), describe() + "-sort",
                this::parse, comparator);
        runs.clear();
        runs.addAll(reduced);
        final int[] plan = SpillSupport.mergePlan(ctx);
        merger = new Merger<>(ctx, runs, owner("merge buffers"), plan[1], this::parse, comparator);
        stats().annotate("sort.spilled", "true");
        stats().annotate("sort.runs", runs.size());
        phase = Phase.MERGE;
    }

    private void spillBuffer() {
        buffer.sort(comparator);
        spillReserve.handOver();
        final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner("run buffer"), describe() + "-run" + runs.size(),
                SpillSupport.bufferBytes(ctx, 4));
        try {
            final Bytes record = new Bytes();
            final Bytes value = new Bytes();
            for (final Rec r : buffer) {
                record.reset();
                record.putLong(r.seq);
                record.putBlock(r.entry);
                record.putInt(r.keys.length);
                for (final Object key : r.keys) {
                    value.reset();
                    codec.encodeObject(key, value);
                    record.putBlock(value.toArray());
                }
                writer.writeRecord(record.toArray());
                ctx.checkInterrupt();
            }
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
        runs.add(writer.finish());
        ctx.budget().releaseAll(owner("run buffer"));
        buffer.clear();
        ctx.budget().releaseAll(owner("sort buffer"));
        spillReserve.retake();
    }

    private Rec parse(final byte[] payload) {
        final ByteSource source = new ByteSource(payload);
        final long seq = source.getLong();
        final byte[] bytes = source.getBlock();
        final Object[] keys = new Object[source.getInt()];
        for (int k = 0; k < keys.length; k++) keys[k] = codec.decodeObject(new ByteSource(source.getBlock()));
        return new Rec(seq, bytes, keys, 0);
    }

    private void clearState() {
        if (merger != null) {
            merger.close();
            merger = null;
        }
        for (final SpillRun run : runs) run.discard(ctx.scratch());
        runs.clear();
        buffer.clear();
        spillReserve.release();
        for (final String state : new String[]{"sort buffer", "run buffer", "merge buffers"}) {
            ctx.budget().releaseAll(owner(state));
        }
        sequence = 0;
        next = 0;
        if (in != null) in.clear();
    }

    @Override
    protected void doReset() {
        clearState();
        spillReserve.hold();
        for (final KeyEvaluator evaluator : keyEvaluators) evaluator.reset();
        phase = Phase.LOAD;
    }

    @Override
    protected void doClose() {
        try {
            clearState();
        } finally {
            for (final KeyEvaluator evaluator : keyEvaluators) evaluator.close();
            keyEvaluators.clear();
        }
    }
}
