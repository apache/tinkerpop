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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.branch;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.List;

/**
 * {@code Local}, {@code FlatMap}, {@code MapFirst}, {@code Coalesce} and {@code Optional}: a child run for each input
 * entry, reset before every entry like the standard steps reset their child traversals.
 * <ul>
 *     <li>{@code LOCAL} and {@code FLAT_MAP}: all results, child fed bulk 1, result bulk times the entry's bulk. (The
 *     standard {@code LocalStep} splits the bulk into units, {@code TraversalFlatMapStep} prepares the child start
 *     with bulk 1; both are the same multiset.)</li>
 *     <li>{@code MAP_FIRST}: the first result only, with the entry's bulk; nothing when the child has none.</li>
 *     <li>{@code COALESCE}: the results of the first branch that has any, child fed bulk 1 like
 *     {@code CoalesceStep}, result bulk times the entry's bulk.</li>
 *     <li>{@code OPTIONAL}: the results of the child fed the entry's real bulk, with their own bulk, or the entry
 *     itself if there are none.</li>
 * </ul>
 * For vertex and edge inputs with a side-effect free child, the results of bulk-1 runs are memoized per ordinal in a
 * bounded {@link ResultCache}, and {@code OPTIONAL} remembers the ordinals whose child is empty.
 */
final class PerEntryOperator extends BranchOperator {

    enum Mode { LOCAL, FLAT_MAP, MAP_FIRST, COALESCE, OPTIONAL }

    private final Mode mode;
    private final List<CsrPlan> plans;

    private ChildRun[] runs;
    private ResultCache cache;
    private OrdinalBits emptyMemo;
    private Batch in;
    private Batch fallback;
    private int position;
    private ChildRun active;
    private boolean recording;
    private int recordOrdinal;
    private long childRuns;
    private long cacheHits;

    PerEntryOperator(final Mode mode, final List<CsrPlan> plans, final OperatorSpec spec) {
        super(spec);
        this.mode = mode;
        this.plans = plans;
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        position = 0;
        runs = new ChildRun[plans.size()];
        boolean sideEffects = false;
        try {
            for (int k = 0; k < runs.length; k++) {
                runs[k] = new ChildRun(ctx, plans.get(k), owner("child " + k));
                sideEffects |= Plans.writesSideEffects(plans.get(k));
            }
            final Lane lane = spec.inputLane();
            final boolean ordinals = (lane == Lane.V || lane == Lane.E) && !sideEffects;
            if (ordinals && mode != Mode.OPTIONAL) cache = ResultCache.tryCreate(ctx, spec.outputLane(), owner("cache"));
            if (ordinals && mode == Mode.OPTIONAL) {
                emptyMemo = OrdinalBits.tryCreate(ctx, Plans.universe(ctx.snapshot(), lane), owner("empty memo"));
            }
            if (mode == Mode.OPTIONAL) fallback = spec.newOutputBatch(1);
        } catch (RuntimeException e) {
            closeResources();
            throw e;
        }
    }

    @Override
    protected boolean advance() {
        while (true) {
            if (active != null) {
                if (active.next()) {
                    record(active.res, 0, active.res.n);
                    setWindow(active.res, 0, active.res.n, curMult);
                    return true;
                }
                active = null;
                endRecord();
            }
            if (position >= in.n) {
                stats().annotate("childRuns", childRuns);
                stats().annotate("cacheHits", cacheHits);
                if (!pull(in)) return false;
                position = 0;
            }
            clearWindow();
            start(position++);
            if (curPos < curEnd) return true;
        }
    }

    private void start(final int i) {
        final long bulk = in.bulk[i];
        final boolean byOrdinal = cache != null || emptyMemo != null;
        final int ordinal = byOrdinal ? in.ord[i] : -1;
        if (cache != null) {
            final long hit = cache.lookup(ordinal);
            if (hit >= 0) {
                cacheHits++;
                final int from = (int) (hit >>> 32);
                final int to = from + (int) hit;
                if (mode == Mode.MAP_FIRST) setFixedWindow(cache.store(), from, to, bulk);
                else setWindow(cache.store(), from, to, bulk);
                return;
            }
        }
        switch (mode) {
            case LOCAL:
            case FLAT_MAP:
                beginRecord(ordinal);
                childRuns++;
                runs[0].begin(in, i, 1L);
                curMult = bulk;
                active = runs[0];
                break;
            case MAP_FIRST:
                beginRecord(ordinal);
                childRuns++;
                runs[0].begin(in, i, 1L);
                if (runs[0].next()) {
                    record(runs[0].res, 0, 1);
                    setFixedWindow(runs[0].res, 0, 1, bulk);
                }
                endRecord();
                break;
            case COALESCE:
                beginRecord(ordinal);
                for (final ChildRun run : runs) {
                    childRuns++;
                    run.begin(in, i, 1L);
                    if (run.next()) {
                        record(run.res, 0, run.res.n);
                        setWindow(run.res, 0, run.res.n, bulk);
                        active = run;
                        return;
                    }
                }
                endRecord();
                break;
            default:
                startOptional(i, ordinal, bulk);
                break;
        }
    }

    private void startOptional(final int i, final int ordinal, final long bulk) {
        if (emptyMemo != null && emptyMemo.get(ordinal) == 1) {
            emitInput(i);
            return;
        }
        childRuns++;
        if (runs[0].exists(in, i, bulk)) {
            setWindow(runs[0].res, 0, runs[0].res.n, 1L);
            active = runs[0];
        } else {
            if (emptyMemo != null) emptyMemo.put(ordinal, true);
            emitInput(i);
        }
    }

    private void emitInput(final int i) {
        fallback.clear();
        fallback.copyEntry(in, i);
        setWindow(fallback, 0, 1, 1L);
    }

    private void beginRecord(final int ordinal) {
        recording = cache != null;
        if (recording) {
            cache.beginRecord();
            recordOrdinal = ordinal;
        }
    }

    private void record(final Batch results, final int from, final int to) {
        if (recording) recording = cache.append(results, from, to);
    }

    private void endRecord() {
        if (recording) cache.commit(recordOrdinal);
        recording = false;
    }

    @Override
    protected void doReset() {
        in.n = 0;
        position = 0;
        active = null;
        recording = false;
        clearWindow();
        if (cache != null) cache.clear();
        if (emptyMemo != null) emptyMemo.clear();
    }

    @Override
    protected void doClose() {
        closeResources();
    }

    private void closeResources() {
        RuntimeException failure = null;
        if (runs != null) {
            for (final ChildRun run : runs) {
                if (run == null) continue;
                try {
                    run.close();
                } catch (RuntimeException e) {
                    if (failure == null) failure = e;
                    else failure.addSuppressed(e);
                }
            }
        }
        if (cache != null) cache.close();
        if (emptyMemo != null) emptyMemo.close();
        runs = null;
        cache = null;
        emptyMemo = null;
        fallback = null;
        in = null;
        active = null;
        clearWindow();
        if (failure != null) throw failure;
    }
}
