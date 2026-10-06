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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.terminal;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.FeedSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Keys;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The state of {@code groupCount} and {@code group}, as a terminal or as a side-effect writer: a {@link GroupIndex} that
 * numbers the keys, and a count or a reducer per group. {@link #build()} makes the result map once the input is
 * consumed; it is a {@code HashMap} like the standard steps' result.
 */
abstract class GroupAccumulator implements AutoCloseable {

    /**
     * Adds the entries of the batch.
     */
    abstract void add(Batch b);

    /**
     * Whether no entry was productive, so that a side-effect writer has nothing to write.
     */
    abstract boolean isEmpty();

    abstract Map<Object, Object> build();

    abstract void reset();

    @Override
    public abstract void close();

    static GroupAccumulator counts(final CsrExecutionContext ctx, final Keys.Key key, final Lane lane,
                                   final String owner, final boolean strict) {
        return new Counts(ctx, key, lane, owner, strict);
    }

    static GroupAccumulator reduced(final CsrExecutionContext ctx, final Keys.Key key, final CsrPlan reducer,
                                    final Lane lane, final String owner) {
        return new Reduced(ctx, key, reducer, lane, owner);
    }

    // ---------------------------------------------------------------- groupCount

    private static final class Counts extends GroupAccumulator {

        private final CsrExecutionContext ctx;
        private final GroupIndex index;
        private final String owner;
        private final boolean strict;
        private long[] counts = new long[16];

        Counts(final CsrExecutionContext ctx, final Keys.Key key, final Lane lane, final String owner,
               final boolean strict) {
            this.ctx = ctx;
            this.owner = owner + " counts";
            this.strict = strict;
            ctx.budget().reserve(8L * counts.length, this.owner);
            this.index = GroupIndex.create(ctx, key, lane, owner + " keys");
        }

        @Override
        void add(final Batch b) {
            for (int i = 0; i < b.n; i++) {
                final int group = index.indexOf(b, i);
                if (group < 0) {
                    // TraversalUtil.applyNullable, which the groupCount('x') writer uses, throws
                    if (strict) throw new IllegalArgumentException(
                            "The provided traverser does not map to a value: the by() modulator of groupCount");
                    continue;
                }
                if (group >= counts.length) {
                    ctx.budget().reserve(8L * counts.length, owner);
                    counts = Arrays.copyOf(counts, counts.length * 2);
                }
                counts[group] += b.bulk[i];
            }
        }

        @Override
        boolean isEmpty() {
            return index.size() == 0;
        }

        @Override
        Map<Object, Object> build() {
            final Map<Object, Object> map = new HashMap<>();
            for (int g = 0; g < index.size(); g++) map.put(index.keyOf(g), counts[g]);
            return map;
        }

        @Override
        void reset() {
            index.reset();
            Arrays.fill(counts, 0);
        }

        @Override
        public void close() {
            index.close();
            ctx.budget().releaseAll(owner);
        }
    }

    // ---------------------------------------------------------------- group

    private static final class Reduced extends GroupAccumulator {

        private static final long REDUCER_BYTES = 128;

        /**
         * The reducer is a terminal over the entries themselves: feed the entries straight to a reducer per group.
         */
        private static final int DIRECT = 0;
        /**
         * The reducer has stateless operators before its terminal: run them over the entries of each group in each
         * batch and feed the results to a reducer per group.
         */
        private static final int PARTITIONED = 1;
        /**
         * The reducer has stateful operators: keep the entries, then run the whole reducer once per group.
         */
        private static final int BUFFERED = 2;

        private final CsrExecutionContext ctx;
        private final GroupIndex index;
        private final CsrPlan plan;
        private final CsrOp terminal;
        private final Lane lane;
        private final String groupsOwner;
        private final String bufferOwner;
        private final int mode;
        private final List<Reducers.Reducer> reducers = new ArrayList<>();

        // PARTITIONED
        private FeedSupplier feed;
        private CsrPipeline body;
        private Batch sub;
        private Batch bodyOut;
        private long[] sortKeys;

        // BUFFERED
        private final List<Batch> chunks = new ArrayList<>();
        private final List<int[]> chunkGroups = new ArrayList<>();
        private RunSupplier runs;
        private CsrPipeline whole;

        Reduced(final CsrExecutionContext ctx, final Keys.Key key, final CsrPlan plan, final Lane lane,
                final String owner) {
            this.ctx = ctx;
            this.plan = plan;
            this.lane = lane;
            this.terminal = plan.terminal();
            if (terminal == null || !Reducers.isReducer(terminal) || plan.inputLane() != lane) {
                throw new IllegalArgumentException("A group reducer must read lane " + lane
                        + " and end in count, fold, sum, min, max or mean: " + plan);
            }
            this.groupsOwner = owner + " groups";
            this.bufferOwner = owner + " buffer";
            this.index = GroupIndex.create(ctx, key, lane, owner + " keys");
            boolean stateful = false;
            for (final CsrOp op : plan.ops()) stateful |= op.isStateful();
            if (plan.ops().isEmpty()) {
                mode = DIRECT;
            } else if (!stateful) {
                mode = PARTITIONED;
                feed = new FeedSupplier();
                body = CsrPipeline.open(ctx, new CsrPlan(plan.nodes().subList(0, plan.nodes().size() - 1)), feed);
                sub = new Batch(lane, ctx.batchSize());
                bodyOut = body.newOutputBatch(ctx.batchSize());
                sortKeys = new long[ctx.batchSize()];
            } else {
                mode = BUFFERED;
                runs = new RunSupplier();
                whole = CsrPipeline.open(ctx, plan, runs);
            }
        }

        private Reducers.Reducer reducer(final int group) {
            while (reducers.size() <= group) {
                ctx.budget().reserve(REDUCER_BYTES, groupsOwner);
                reducers.add(Reducers.create(terminal, ctx, groupsOwner));
            }
            return reducers.get(group);
        }

        @Override
        void add(final Batch b) {
            switch (mode) {
                case DIRECT:
                    for (int i = 0; i < b.n; i++) {
                        final int group = index.indexOf(b, i);
                        if (group >= 0) reducer(group).add(b, i);
                    }
                    break;
                case PARTITIONED:
                    addPartitioned(b);
                    break;
                default:
                    addBuffered(b);
                    break;
            }
        }

        private void addPartitioned(final Batch b) {
            if (sortKeys.length < b.n) sortKeys = new long[b.n];
            int m = 0;
            for (int i = 0; i < b.n; i++) {
                final int group = index.indexOf(b, i);
                if (group < 0) continue;
                reducer(group);
                sortKeys[m++] = ((long) group << 32) | i;
            }
            Arrays.sort(sortKeys, 0, m);
            int p = 0;
            while (p < m) {
                ctx.checkInterrupt();
                final int group = (int) (sortKeys[p] >>> 32);
                sub.clear();
                while (p < m && (int) (sortKeys[p] >>> 32) == group) {
                    sub.copyEntry(b, (int) sortKeys[p]);
                    p++;
                    if (sub.isFull()) {
                        runBody(group);
                        sub.clear();
                    }
                }
                if (!sub.isEmpty()) runBody(group);
            }
        }

        private void runBody(final int group) {
            feed.set(sub);
            body.reset();
            final Reducers.Reducer reducer = reducers.get(group);
            while (body.next(bodyOut)) reducer.addAll(bodyOut);
        }

        private void addBuffered(final Batch b) {
            for (int i = 0; i < b.n; i++) {
                final int group = index.indexOf(b, i);
                if (group < 0) continue;
                Batch chunk = chunks.isEmpty() ? null : chunks.get(chunks.size() - 1);
                if (chunk == null || chunk.isFull()) {
                    chunk = new Batch(lane, ctx.batchSize());
                    ctx.budget().reserve(chunk.estimatedBytes() + 4L * chunk.capacity, bufferOwner);
                    chunks.add(chunk);
                    chunkGroups.add(new int[chunk.capacity]);
                }
                chunk.copyEntry(b, i);
                chunkGroups.get(chunkGroups.size() - 1)[chunk.n - 1] = group;
            }
        }

        @Override
        boolean isEmpty() {
            return index.size() == 0;
        }

        @Override
        Map<Object, Object> build() {
            final Map<Object, Object> map = new HashMap<>();
            if (mode != BUFFERED) {
                for (int g = 0; g < index.size(); g++) {
                    final Reducers.Reducer reducer = reducers.get(g);
                    if (reducer.hasResult()) map.put(index.keyOf(g), reducer.result());
                }
                return map;
            }
            if (chunks.isEmpty()) return map;
            final int capacity = chunks.get(0).capacity;
            final long total = (long) (chunks.size() - 1) * capacity + chunks.get(chunks.size() - 1).n;
            ctx.budget().reserve(8L * total, bufferOwner);
            final long[] order = new long[(int) total];
            for (int p = 0; p < total; p++) {
                order[p] = ((long) chunkGroups.get(p / capacity)[p % capacity] << 32) | p;
            }
            Arrays.sort(order);
            final Batch result = whole.newOutputBatch(1);
            int p = 0;
            while (p < total) {
                ctx.checkInterrupt();
                final int group = (int) (order[p] >>> 32);
                int q = p;
                while (q < total && (int) (order[q] >>> 32) == group) q++;
                runs.set(chunks, capacity, order, p, q);
                whole.reset();
                if (whole.next(result) && result.n > 0) map.put(index.keyOf(group), result.val[0]);
                p = q;
            }
            return map;
        }

        @Override
        void reset() {
            index.reset();
            reducers.clear();
            chunks.clear();
            chunkGroups.clear();
            ctx.budget().releaseAll(groupsOwner);
            ctx.budget().releaseAll(bufferOwner);
        }

        @Override
        public void close() {
            if (body != null) body.close();
            if (whole != null) whole.close();
            index.close();
            reducers.clear();
            chunks.clear();
            chunkGroups.clear();
            ctx.budget().releaseAll(groupsOwner);
            ctx.budget().releaseAll(bufferOwner);
        }
    }

    /**
     * Serves the entries of one group from the buffered chunks, in arrival order.
     */
    private static final class RunSupplier implements BatchSupplier {

        private List<Batch> chunks;
        private int capacity;
        private long[] order;
        private int position;
        private int end;

        void set(final List<Batch> chunks, final int capacity, final long[] order, final int start, final int end) {
            this.chunks = chunks;
            this.capacity = capacity;
            this.order = order;
            this.position = start;
            this.end = end;
        }

        @Override
        public boolean next(final Batch out) {
            out.clear();
            if (chunks == null || position >= end) return false;
            while (position < end && !out.isFull()) {
                final int p = (int) order[position++];
                out.copyEntry(chunks.get(p / capacity), p % capacity);
            }
            return true;
        }
    }
}
