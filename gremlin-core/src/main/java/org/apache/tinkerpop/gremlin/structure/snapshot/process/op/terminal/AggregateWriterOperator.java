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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillBuffer;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillCountTable;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillFrontier;

import java.util.function.BinaryOperator;

/**
 * {@code AggregateSideEffect}: a barrier that consumes the whole input, writes it to the side effect as a
 * {@link BulkSet} and then re-emits it, so a later reader sees the complete set. The write follows
 * {@code AggregateStep}: a {@code BulkSet} through {@code sideEffects.add(key, bulkSet)} when the registered reducer is
 * {@code Operator.addAll} or {@code Operator.assign}, otherwise one add per element. Nothing is written for an empty
 * input.
 * <p>
 * The input is kept in a {@link SpillBuffer}, in memory up to the quota and on scratch beyond it, and replayed after the
 * write. The bulks are counted per distinct element by a {@link SpillFrontier} (ordinals of a V or E input) or a
 * {@link SpillCountTable} (any other lane), which stay within the quota too. Only the {@code BulkSet} itself, the side
 * effect, is unavoidable state and is reserved from the shared budget entry by entry, built from the drained counts.
 */
final class AggregateWriterOperator extends AbstractCsrOperator {

    /**
     * A {@code BulkSet} entry: a {@code LinkedHashMap} node (about 40 bytes), its table slot, the boxed {@code Long}
     * bulk and the element or value object.
     */
    private static final long BULK_SET_ENTRY_BYTES = 128;

    private final String sideEffectKey;
    private Batch in;
    private SpillBuffer buffer;
    private SpillFrontier frontier;
    private SpillCountTable table;
    private BatchSupplier replay;
    private boolean drained;

    AggregateWriterOperator(final OperatorSpec spec) {
        super(spec);
        this.sideEffectKey = ((Ops.AggregateSideEffect) spec.node()).sideEffectKey();
    }

    @Override
    protected void doOpen() {
        if (ctx.sideEffects() == null) {
            throw new IllegalStateException(describe() + " needs the side effects of a traversal");
        }
        in = spec.newInputBatch(ctx.batchSize());
        buffer = new SpillBuffer(ctx, owner("buffer"), spec.inputLane(), spec.inputRecordsSource());
    }

    @Override
    protected boolean produce(final Batch out) {
        if (!drained) drain();
        if (replay == null) replay = buffer.replay();
        return replay.next(out);
    }

    private void drain() {
        drained = true;
        final Lane lane = spec.inputLane();
        if (lane.isElement()) {
            final int universe = lane == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
            // a dense frontier is long[universe]: ask for it only if it fits the quota
            frontier = SpillFrontier.create(ctx, owner("counts"), lane, universe,
                    8L * universe <= ctx.quota() ? universe : 0);
        } else {
            table = new SpillCountTable(ctx, owner("counts"));
        }
        while (pull(in)) {
            for (int i = 0; i < in.n; i++) {
                buffer.add(in, i);
                if (table != null) table.add(in, i);
            }
            if (frontier != null) frontier.addAll(in);
            ctx.checkInterrupt();
        }
        buffer.finish();
        if (buffer.isEmpty()) {
            releaseCounts();
            return;
        }
        final BulkSet<Object> set = new BulkSet<>();
        if (frontier != null) {
            final Batch counted = new Batch(lane, Batch.capacityWithin(lane, false, ctx.batchSize(),
                    ctx.budget().available() / 8), false);
            ctx.budget().reserve(counted.estimatedBytes(), owner("counted"));
            frontier.seal();
            while (frontier.drain(counted) > 0) {
                for (int i = 0; i < counted.n; i++) {
                    ctx.budget().reserve(BULK_SET_ENTRY_BYTES, owner("set"));
                    set.add(lane == Lane.V ? ctx.graph().vertexAt(counted.ord[i]) : ctx.graph().edgeAt(counted.ord[i]),
                            counted.bulk[i]);
                }
                counted.clear();
                ctx.checkInterrupt();
            }
            ctx.budget().releaseAll(owner("counted"));
        } else {
            table.drain((element, count) -> {
                ctx.budget().reserve(BULK_SET_ENTRY_BYTES + valueBytes(element), owner("set"));
                set.add(element, count);
            });
        }
        releaseCounts();
        final BinaryOperator<Object> reducer = ctx.sideEffects().getReducer(sideEffectKey);
        if (reducer == Operator.addAll || reducer == Operator.assign) {
            ctx.sideEffects().add(sideEffectKey, set);
        } else {
            for (final Object element : set) ctx.sideEffects().add(sideEffectKey, element);
        }
    }

    private static long valueBytes(final Object element) {
        return element instanceof CharSequence ? 40L + 2L * ((CharSequence) element).length() : 0;
    }

    private void releaseCounts() {
        if (frontier != null) {
            frontier.release();
            frontier = null;
        }
        if (table != null) {
            table.close();
            table = null;
        }
    }

    @Override
    protected void doReset() {
        release();
        in.clear();
    }

    @Override
    protected void doClose() {
        release();
        if (buffer != null) buffer.close();
        buffer = null;
        in = null;
    }

    private void release() {
        releaseCounts();
        if (buffer != null) buffer.clear();
        replay = null;
        drained = false;
        ctx.budget().releaseAll(owner("counted"));
        ctx.budget().releaseAll(owner("set"));
    }
}
