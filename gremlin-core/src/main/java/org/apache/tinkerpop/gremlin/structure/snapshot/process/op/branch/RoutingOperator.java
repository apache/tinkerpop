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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.BatchSupplier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill.SpillBuffer;

import java.util.ArrayList;
import java.util.List;

/**
 * The base of {@code Union} and {@code Choose}, whose branch traversals are not reset between traversers: a branch
 * sees the stream of all entries routed to it. Entries keep their incoming bulk.
 * <p>
 * A branch without state across entries (see {@link Plans#holdsState}) is run over each input batch as it arrives: the
 * batch is routed to the branches, each branch is reset, fed and drained, so memory stays at one batch per branch. A
 * branch with state ({@code union(count())}, {@code dedup()} in a branch, a range) needs its whole input before it can
 * finish, so the entries routed to it are retained in a {@link SpillBuffer}, in memory up to the operator's quota and
 * on scratch beyond it, and the branch runs over a replay of them once the input is exhausted.
 */
abstract class RoutingOperator extends BranchOperator {

    /**
     * The entries kept for stateful branches, in memory up to the quota and on scratch beyond it.
     */
    static final class Retention {

        private final SpillBuffer buffer;

        private Retention(final RoutingOperator operator, final String owner) {
            this.buffer = new SpillBuffer(operator.ctx, owner, operator.spec.inputLane(), false);
        }

        void add(final Batch source, final int i) {
            buffer.add(source, i);
        }

        boolean isEmpty() {
            return buffer.isEmpty();
        }

        /**
         * The retained entries from the start; may be taken again for each stateful branch.
         */
        BatchSupplier replay() {
            return buffer.replay();
        }

        void release() {
            buffer.clear();
        }

        void close() {
            buffer.close();
        }
    }

    private static final int STREAM = 0;
    private static final int REPLAY = 1;

    private final List<CsrPlan> plans;

    protected ChildRun[] runs;
    protected boolean[] streaming;
    /**
     * The retention of each stateful branch, null for the others. Branches may share one.
     */
    protected Retention[] retention;
    protected Batch in;

    private List<Retention> retentions;
    private ChildRun active;
    private int phase;
    private int branch;
    private boolean chunkReady;

    protected RoutingOperator(final List<CsrPlan> plans, final OperatorSpec spec) {
        super(spec);
        this.plans = plans;
    }

    /**
     * The batch that holds the entries routed to a streaming branch for the current input batch, or null if none.
     */
    protected abstract Batch batchFor(int branch);

    /**
     * Routes the entries of a new input batch: fills what {@link #batchFor} returns and adds entries to the retention
     * of the stateful branches.
     */
    protected abstract void route(Batch batch);

    /**
     * Called by subclasses from {@code onOpen} once {@link #retention} needs to be populated: creates the retentions.
     * With {@code shared}, all stateful branches share a single one.
     */
    protected final void openRuns(final boolean shared) {
        in = spec.newInputBatch(ctx.batchSize());
        runs = new ChildRun[plans.size()];
        streaming = new boolean[plans.size()];
        retention = new Retention[plans.size()];
        retentions = new ArrayList<>();
        try {
            Retention common = null;
            for (int k = 0; k < runs.length; k++) {
                runs[k] = new ChildRun(ctx, plans.get(k), owner("child " + k));
                streaming[k] = !Plans.holdsState(plans.get(k));
                if (!streaming[k]) {
                    if (shared && common != null) {
                        retention[k] = common;
                    } else {
                        retention[k] = new Retention(this, owner("retained " + k));
                        retentions.add(retention[k]);
                        if (shared) common = retention[k];
                    }
                }
            }
        } catch (RuntimeException e) {
            closeRuns();
            throw e;
        }
        restart();
    }

    private void restart() {
        in.n = 0;
        active = null;
        phase = STREAM;
        branch = 0;
        chunkReady = false;
        clearWindow();
        for (final Retention r : retentions) r.release();
    }

    @Override
    protected boolean advance() {
        while (true) {
            if (active != null) {
                if (active.next()) {
                    setWindow(active.res, 0, active.res.n, 1L);
                    return true;
                }
                active = null;
                branch++;
            }
            if (phase == STREAM) {
                while (chunkReady && branch < runs.length) {
                    if (streaming[branch]) {
                        final Batch routed = batchFor(branch);
                        if (routed != null && routed.n > 0) {
                            runs[branch].beginBatch(routed);
                            active = runs[branch];
                            break;
                        }
                    }
                    branch++;
                }
                if (active != null) continue;
                if (!pull(in)) {
                    phase = REPLAY;
                    branch = 0;
                    chunkReady = false;
                    continue;
                }
                route(in);
                branch = 0;
                chunkReady = true;
            } else {
                // a branch that received no entry is never evaluated, like a branch traversal without starts
                while (branch < runs.length && (streaming[branch] || retention[branch].isEmpty())) branch++;
                if (branch >= runs.length) {
                    for (final Retention r : retentions) r.release();
                    return false;
                }
                runs[branch].beginReplay(retention[branch].replay());
                active = runs[branch];
            }
        }
    }

    @Override
    protected void doReset() {
        restart();
    }

    @Override
    protected void doClose() {
        closeRuns();
    }

    /**
     * Closes everything the subclass does not own.
     */
    protected void closeRuns() {
        RuntimeException failure = null;
        if (retentions != null) {
            for (final Retention r : retentions) r.close();
        }
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
        runs = null;
        retentions = null;
        in = null;
        active = null;
        clearWindow();
        if (failure != null) throw failure;
    }
}
