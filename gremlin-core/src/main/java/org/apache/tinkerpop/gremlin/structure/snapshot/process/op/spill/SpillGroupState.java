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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Materializer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal.Reducers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The state of {@code group().by(key).by(reducer)} with a size that is bounded by the memory budget, shared by the
 * terminal ({@link SpillGroupOperator}) and the side-effect writer ({@link SpillGroupWriterOperator}). The result is a
 * {@code HashMap} from the key to the value the reducer plan returns for the members of the group, which it sees in
 * arrival order with their bulk.
 * <p>
 * There are two ways to keep a group, chosen from the reducer plan.
 * <ul>
 *   <li><b>Partial state</b>, for a reducer of count, sum, min, max or mean whose operators before the terminal are all
 *   stateless. The table holds one {@link Reducers.Partial} per key, so it grows with the number of keys and not with the
 *   number of members. The operators before the terminal run over the members of a key in each batch, and their output
 *   is added to the key's state.</li>
 *   <li><b>Members</b>, for every other reducer (fold, or stateful operators such as {@code dedup()}): the table holds the
 *   serialized members of each key ({@link EntryCodec}).</li>
 * </ul>
 * When the table cannot grow, its keys are sorted by their canonical bytes ({@link KeyCodec}) and written as one run on
 * scratch, as {@code [keyLen][key][repLen][rep][state objects]} per key for partial states and as
 * {@code [keyLen][key][repLen][rep][entry]} per member otherwise, members of a key in arrival order, and the table starts
 * again. At the end the runs are merged by key ({@link Merger}, which keeps arrival order among equal keys because the
 * runs are passed in creation order) and streamed: partial states of a key are combined one after the other, and the
 * members of a key are fed from the merge into the reducer pipeline through a {@link BatchSupplier}, so that the members
 * of one group never sit in memory together. The merge needs no partitioning, so a single key that is larger than the
 * budget is no different from any other. What stays in memory is the result map (see {@link ResultMap}) and the state of
 * the reducer itself, such as the list of a fold.
 */
final class SpillGroupState implements AutoCloseable {

    /**
     * A table entry: a key with its partial state or its members.
     */
    private static final class Slot {
        final byte[] canon;
        final byte[] rep;
        final int id;
        final Reducers.Partial partial;
        final List<byte[]> entries;
        long bytes;

        Slot(final byte[] canon, final byte[] rep, final int id, final Reducers.Partial partial) {
            this.canon = canon;
            this.rep = rep;
            this.id = id;
            this.partial = partial;
            this.entries = partial == null ? new ArrayList<>() : null;
        }
    }

    /**
     * A record of a run, parsed as far as the merge needs.
     */
    private static final class Rec {
        final byte[] payload;
        final byte[] canon;
        final byte[] rep;
        final int offset;

        Rec(final byte[] payload) {
            this.payload = payload;
            final ByteSource source = new ByteSource(payload);
            this.canon = source.getBlock();
            this.rep = source.getBlock();
            this.offset = source.position();
        }
    }

    private static int compare(final byte[] a, final byte[] b) {
        final int n = Math.min(a.length, b.length);
        for (int i = 0; i < n; i++) {
            final int c = (a[i] & 0xFF) - (b[i] & 0xFF);
            if (c != 0) return c;
        }
        return Integer.compare(a.length, b.length);
    }

    /**
     * Serves the members of one group from a list.
     */
    private static final class ListSupplier implements BatchSupplier {
        private final EntryCodec entries;
        private List<byte[]> members = List.of();
        private int index;

        ListSupplier(final EntryCodec entries) {
            this.entries = entries;
        }

        void set(final List<byte[]> list) {
            members = list;
            index = 0;
        }

        @Override
        public boolean next(final Batch out) {
            out.clear();
            while (!out.isFull() && index < members.size()) {
                entries.decode(new ByteSource(members.get(index++)), out);
            }
            return out.n > 0;
        }
    }

    /**
     * Serves the members of the current key of a merge, in the order of the merge, and ends at the next key.
     */
    private static final class StreamSupplier implements BatchSupplier {
        private final EntryCodec entries;
        private Merger<Rec> merger;
        private boolean has;
        private byte[] key;

        StreamSupplier(final EntryCodec entries) {
            this.entries = entries;
        }

        void start(final Merger<Rec> merger) {
            this.merger = merger;
            this.has = merger.advance();
        }

        boolean hasCurrent() {
            return has;
        }

        Rec current() {
            return merger.value();
        }

        void begin(final byte[] groupKey) {
            key = groupKey;
        }

        /**
         * Moves past the members of the current key that the reducer did not read.
         */
        void finishGroup() {
            while (has && Arrays.equals(merger.value().canon, key)) has = merger.advance();
        }

        @Override
        public boolean next(final Batch out) {
            out.clear();
            while (!out.isFull() && has && Arrays.equals(merger.value().canon, key)) {
                final Rec r = merger.value();
                entries.decode(new ByteSource(r.payload, r.offset), out);
                has = merger.advance();
            }
            return out.n > 0;
        }
    }

    private static final class SwitchSupplier implements BatchSupplier {
        BatchSupplier target;

        @Override
        public boolean next(final Batch out) {
            out.clear();
            return target != null && target.next(out);
        }
    }

    private final CsrExecutionContext ctx;
    private final OperatorStats stats;
    private final String base;
    private final CsrPlan reducerPlan;
    private final CsrOp terminal;
    private final boolean partialMode;

    private final KeyCodec codec;
    private final KeyEvaluator keys;
    private final Bytes canon = new Bytes();
    private final Bytes rep = new Bytes();
    private final Bytes entry = new Bytes();

    // members
    private EntryCodec entries;
    private ListSupplier listSupplier;
    private StreamSupplier streamSupplier;
    private SwitchSupplier supplier;
    private CsrPipeline reducer;
    private Batch reducerOut;

    // partial states with operators before the terminal
    private FeedSupplier feed;
    private CsrPipeline body;
    private Batch sub;
    private Batch bodyOut;
    private Slot[] pendingSlots;
    private int[] pendingIndex;
    private long[] sortKeys;

    private final SpillReserve spillReserve;
    private final Map<KeyBytes, Slot> table = new HashMap<>();
    private final List<SpillRun> runs = new ArrayList<>();
    private final ResultMap result;
    private int nextSlotId;
    private boolean any;

    /**
     * @param base the owner prefix, the description of the operator
     */
    SpillGroupState(final CsrExecutionContext ctx, final OperatorSpec spec, final OperatorStats stats,
                    final String base, final Keys.Key key, final CsrPlan reducerPlan) {
        this.ctx = ctx;
        this.stats = stats;
        this.base = base;
        this.reducerPlan = reducerPlan;
        this.terminal = reducerPlan.terminal();
        boolean stateful = false;
        for (final CsrOp op : reducerPlan.ops()) stateful |= op.isStateful();
        this.partialMode = terminal != null && Reducers.isPartial(terminal) && !stateful;
        // headroom for the run writer, handed over in spillTable
        spillReserve = new SpillReserve(ctx, owner("spill reserve"), SpillReserve.RUN);
        spillReserve.hold();
        codec = new KeyCodec(ctx);
        keys = new KeyEvaluator(ctx, codec, key, spec.inputLane());
        keys.open();
        result = new ResultMap(ctx.budget(), owner("result map"));
        if (partialMode) {
            if (!reducerPlan.ops().isEmpty()) {
                feed = new FeedSupplier();
                body = CsrPipeline.open(ctx, new CsrPlan(reducerPlan.nodes().subList(0, reducerPlan.nodes().size() - 1)),
                        feed);
                sub = new Batch(spec.inputLane(), ctx.batchSize());
                bodyOut = body.newOutputBatch(ctx.batchSize());
                pendingSlots = new Slot[ctx.batchSize()];
                pendingIndex = new int[ctx.batchSize()];
                sortKeys = new long[ctx.batchSize()];
            }
        } else {
            entries = new EntryCodec(codec, spec.inputLane(), spec.inputRecordsSource());
            listSupplier = new ListSupplier(entries);
            streamSupplier = new StreamSupplier(entries);
            supplier = new SwitchSupplier();
            reducer = CsrPipeline.open(ctx, reducerPlan, supplier);
            reducerOut = reducer.newOutputBatch(ctx.batchSize());
        }
        stats.annotate("group.mode", partialMode ? "partial" : "members");
    }

    private String owner(final String state) {
        return base + " " + state;
    }

    /**
     * Whether no entry had a productive key, so that a side-effect writer has nothing to write.
     */
    boolean isEmpty() {
        return !any;
    }

    // ---------------------------------------------------------------- accumulate

    /**
     * Adds the entries of the batch.
     */
    void add(final Batch in) {
        if (partialMode) addPartial(in);
        else for (int i = 0; i < in.n; i++) addMember(in, i);
    }

    private void addMember(final Batch in, final int i) {
        if (!keys.evaluate(in, i)) return;
        any = true;
        canon.reset();
        rep.reset();
        keys.encode(canon, rep);
        entry.reset();
        entries.encode(in, i, in.bulk[i], entry);
        final byte[] key = canon.toArray();
        Slot m = table.get(new KeyBytes(key, KeyBytes.EMPTY));
        long cost = SpillSupport.RECORD + entry.size() + (m == null ? SpillSupport.MAP_ENTRY + key.length + rep.size() : 0);
        if (!ctx.tryReserveWithinQuota(cost, owner("group table"))) {
            if (!table.isEmpty()) {
                spillTable();
                m = null;
                cost = SpillSupport.RECORD + entry.size() + SpillSupport.MAP_ENTRY + key.length + rep.size();
            }
            ctx.budget().reserve(cost, owner("group table"));
        }
        final byte[] member = entry.toArray();
        if (m == null) {
            final byte[] repBytes = rep.size() == 0 ? KeyBytes.EMPTY : rep.toArray();
            m = new Slot(key, repBytes, nextSlotId++, null);
            table.put(new KeyBytes(key, repBytes), m);
            m.bytes = cost;
        } else {
            m.bytes += cost;
        }
        m.entries.add(member);
    }

    private void addPartial(final Batch in) {
        if (body != null && pendingIndex.length < in.n) {
            pendingSlots = new Slot[in.n];
            pendingIndex = new int[in.n];
            sortKeys = new long[in.n];
        }
        int pending = 0;
        for (int i = 0; i < in.n; i++) {
            if (!keys.evaluate(in, i)) continue;
            any = true;
            canon.reset();
            rep.reset();
            keys.encode(canon, rep);
            final byte[] key = canon.toArray();
            Slot slot = table.get(new KeyBytes(key, KeyBytes.EMPTY));
            if (slot == null) {
                final Reducers.Partial partial = Reducers.createPartial(terminal, ctx);
                final long cost = SpillSupport.MAP_ENTRY + key.length + rep.size() + partial.estimatedBytes();
                if (!ctx.tryReserveWithinQuota(cost, owner("group table"))) {
                    if (!table.isEmpty()) {
                        // the entries seen so far belong to slots that are about to leave the table
                        flushPending(in, pending);
                        pending = 0;
                        spillTable();
                    }
                    ctx.budget().reserve(cost, owner("group table"));
                }
                final byte[] repBytes = rep.size() == 0 ? KeyBytes.EMPTY : rep.toArray();
                slot = new Slot(key, repBytes, nextSlotId++, partial);
                slot.bytes = cost;
                table.put(new KeyBytes(key, repBytes), slot);
            }
            if (body == null) {
                slot.partial.add(in, i);
            } else {
                pendingSlots[pending] = slot;
                pendingIndex[pending++] = i;
            }
        }
        flushPending(in, pending);
    }

    /**
     * Runs the operators before the terminal over the pending entries, group by group, and adds the output to the states.
     */
    private void flushPending(final Batch in, final int count) {
        if (body == null || count == 0) return;
        for (int p = 0; p < count; p++) sortKeys[p] = ((long) pendingSlots[p].id << 32) | p;
        Arrays.sort(sortKeys, 0, count);
        int p = 0;
        while (p < count) {
            ctx.checkInterrupt();
            final Slot slot = pendingSlots[(int) sortKeys[p]];
            sub.clear();
            while (p < count && pendingSlots[(int) sortKeys[p]] == slot) {
                sub.copyEntry(in, pendingIndex[(int) sortKeys[p]]);
                p++;
                if (sub.isFull()) {
                    runBody(slot);
                    sub.clear();
                }
            }
            if (!sub.isEmpty()) runBody(slot);
        }
        Arrays.fill(pendingSlots, 0, count, null);
    }

    private void runBody(final Slot slot) {
        feed.set(sub);
        body.reset();
        while (body.next(bodyOut)) slot.partial.addAll(bodyOut);
    }

    // ---------------------------------------------------------------- spill

    private void head(final Bytes record, final Slot slot) {
        record.putBlock(slot.canon);
        record.putBlock(slot.rep);
    }

    /**
     * Writes the table as one run, keys in canonical order, and empties it.
     */
    private void spillTable() {
        if (table.isEmpty()) return;
        final List<Slot> sorted = new ArrayList<>(table.values());
        sorted.sort((a, b) -> compare(a.canon, b.canon));
        spillReserve.handOver();
        final SpillRun.Writer writer = new SpillRun.Writer(ctx, owner("run writer"), base + "-group" + runs.size(),
                SpillSupport.bufferBytes(ctx, 4));
        try {
            final Bytes record = new Bytes();
            for (final Slot slot : sorted) {
                if (partialMode) {
                    record.reset();
                    head(record, slot);
                    final Object[] state = slot.partial.state();
                    record.putInt(state.length);
                    for (final Object o : state) codec.encodeObject(o, record);
                    writer.writeRecord(record.toArray());
                } else {
                    for (final byte[] member : slot.entries) {
                        record.reset();
                        head(record, slot);
                        record.putBytes(member);
                        writer.writeRecord(record.toArray());
                    }
                }
                ctx.checkInterrupt();
            }
        } catch (RuntimeException e) {
            writer.abort();
            throw e;
        }
        runs.add(writer.finish());
        table.clear();
        ctx.budget().releaseAll(owner("group table"));
        spillReserve.retake();
        stats.annotate("group.spilled", "true");
        stats.annotate("group.runs", runs.size());
    }

    // ---------------------------------------------------------------- result

    /**
     * Builds the result once the input is consumed; the map's budget stays reserved until {@link #reset()} or
     * {@link #close()}, since the consumer keeps the map.
     */
    Map<Object, Object> build() {
        if (runs.isEmpty()) {
            final Iterator<Slot> it = table.values().iterator();
            while (it.hasNext()) {
                final Slot slot = it.next();
                // release the table entry before the result entry is reserved
                it.remove();
                ctx.budget().release(slot.bytes, owner("group table"));
                if (partialMode) emitPartial(slot);
                else emitMembers(slot);
            }
            ctx.budget().releaseAll(owner("group table"));
        } else {
            spillTable();
            spillReserve.release();
            merge();
        }
        return result.handOff();
    }

    private void emitPartial(final Slot slot) {
        if (!slot.partial.hasResult()) return;
        result.put(codec.decodeKey(slot.canon, slot.rep), slot.partial.result());
        ctx.checkInterrupt();
    }

    private void emitMembers(final Slot slot) {
        listSupplier.set(slot.entries);
        supplier.target = listSupplier;
        reducer.reset();
        if (!reducer.next(reducerOut)) return;
        result.put(codec.decodeKey(slot.canon, slot.rep), Materializer.value(ctx, reducerOut, 0));
        ctx.checkInterrupt();
    }

    private void merge() {
        final List<SpillRun> merged = Merger.reduce(ctx, runs, owner("run merge"), base + "-groupmerge", Rec::new,
                (a, b) -> compare(a.canon, b.canon));
        runs.clear();
        runs.addAll(merged);
        try (Merger<Rec> merger = new Merger<>(ctx, runs, owner("run reader"), SpillSupport.mergePlan(ctx)[1], Rec::new,
                (a, b) -> compare(a.canon, b.canon))) {
            if (partialMode) mergePartials(merger);
            else mergeMembers(merger);
        } finally {
            for (final SpillRun run : runs) run.discard(ctx.scratch());
            runs.clear();
        }
    }

    private void mergePartials(final Merger<Rec> merger) {
        boolean more = merger.advance();
        while (more) {
            final Rec first = merger.value();
            final Reducers.Partial partial = Reducers.createPartial(terminal, ctx);
            do {
                final Rec r = merger.value();
                final ByteSource source = new ByteSource(r.payload, r.offset);
                final Object[] state = new Object[source.getInt()];
                for (int k = 0; k < state.length; k++) state[k] = codec.decodeObject(source);
                partial.combine(state);
                more = merger.advance();
                ctx.checkInterrupt();
            } while (more && Arrays.equals(merger.value().canon, first.canon));
            if (partial.hasResult()) result.put(codec.decodeKey(first.canon, first.rep), partial.result());
        }
    }

    private void mergeMembers(final Merger<Rec> merger) {
        supplier.target = streamSupplier;
        streamSupplier.start(merger);
        while (streamSupplier.hasCurrent()) {
            final Rec first = streamSupplier.current();
            streamSupplier.begin(first.canon);
            reducer.reset();
            if (reducer.next(reducerOut)) {
                result.put(codec.decodeKey(first.canon, first.rep), Materializer.value(ctx, reducerOut, 0));
            }
            streamSupplier.finishGroup();
            ctx.checkInterrupt();
        }
        supplier.target = null;
    }

    // ---------------------------------------------------------------- lifecycle

    private void clearState() {
        for (final SpillRun run : runs) run.discard(ctx.scratch());
        runs.clear();
        spillReserve.release();
        table.clear();
        result.clear();
        if (pendingSlots != null) Arrays.fill(pendingSlots, null);
        if (supplier != null) supplier.target = null;
        for (final String state : new String[]{"group table", "run writer", "run merge", "run reader"}) {
            ctx.budget().releaseAll(owner(state));
        }
    }

    /**
     * Back to the state after construction: nothing accumulated, the result released.
     */
    void reset() {
        clearState();
        spillReserve.hold();
        keys.reset();
        nextSlotId = 0;
        any = false;
    }

    @Override
    public void close() {
        try {
            clearState();
        } finally {
            try {
                keys.close();
            } finally {
                try {
                    if (body != null) {
                        final CsrPipeline b = body;
                        body = null;
                        b.close();
                    }
                } finally {
                    if (reducer != null) {
                        final CsrPipeline r = reducer;
                        reducer = null;
                        r.close();
                    }
                }
            }
        }
    }
}
