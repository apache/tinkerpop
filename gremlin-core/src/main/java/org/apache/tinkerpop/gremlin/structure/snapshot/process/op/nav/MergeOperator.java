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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.nav;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.AbstractCsrOperator;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.DenseFrontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Frontier;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.OperatorSpec;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.SparseFrontier;

/**
 * Where a {@code NoOpBarrierStep} stood: adds the bulks of equal ordinals. The input is accumulated in a
 * {@link Frontier} and emitted in ascending ordinal order. The frontier starts sparse, with a buffer capped by the
 * operator's quota, and switches to the dense representation once the number of input entries makes a dense array the
 * better fit and the array fits within the quota. When the sparse buffer is full it is sealed, its merged entries are
 * emitted, and it is cleared before the input is pulled on: merging fewer entries at a time never changes the result,
 * only how many entries the output holds, so the operator never fails for its merge state. The frontier is released as
 * soon as it is drained.
 */
final class MergeOperator extends AbstractCsrOperator {

    private Batch in;
    private int inPos;
    private Frontier frontier;
    private SparseFrontier sparse;
    private int universe;
    private long added;
    private boolean upgradeTried;
    private boolean inputDone;
    private boolean draining;

    MergeOperator(final OperatorSpec spec) {
        super(spec);
        if (spec.inputRecordsSource()) {
            throw new IllegalStateException("Merge cannot merge edges that record their source vertex");
        }
    }

    @Override
    protected void doOpen() {
        in = spec.newInputBatch(ctx.batchSize());
        universe = spec.inputLane() == Lane.V ? ctx.snapshot().vertexCount() : ctx.snapshot().edgeCount();
        startFrontier();
    }

    private void startFrontier() {
        sparse = new SparseFrontier(ctx.budget(), owner("frontier"), spec.inputLane(), universe, ctx.quota());
        frontier = sparse;
        in.clear();
        inPos = 0;
        added = 0;
        upgradeTried = false;
        inputDone = false;
        draining = false;
    }

    @Override
    protected boolean produce(final Batch out) {
        if (frontier == null) return false;
        while (true) {
            if (draining) {
                if (frontier.drain(out) > 0) return true;
                draining = false;
                if (inputDone) {
                    frontier.release();
                    frontier = null;
                    sparse = null;
                    return false;
                }
                frontier.clear();
                added = 0;
            }
            while (inPos < in.n) {
                if (sparse != null) {
                    if (!sparse.tryAdd(in.ord[inPos], in.bulk[inPos])) break;
                } else {
                    frontier.add(in.ord[inPos], in.bulk[inPos]);
                }
                inPos++;
                added++;
            }
            if (inPos < in.n) {
                frontier.seal();
                draining = true;
                continue;
            }
            if (!inputDone && pull(in)) {
                inPos = 0;
                if (!upgradeTried && sparse != null && added * Frontier.DENSE_RATIO >= universe) {
                    upgradeTried = true;
                    upgrade();
                }
                ctx.checkInterrupt();
                continue;
            }
            inputDone = true;
            frontier.seal();
            draining = true;
        }
    }

    // replaces the sparse frontier with a dense one when the array fits within the quota; otherwise stays sparse
    private void upgrade() {
        final long bytes = DenseFrontier.bytesFor(universe);
        if (bytes > ctx.budget().available() || bytes > ctx.quota()) return;
        final Frontier dense = new DenseFrontier(ctx.budget(), owner("frontier"), spec.inputLane(), universe);
        try {
            sparse.seal();
            final Batch chunk = new Batch(spec.inputLane(), ctx.batchSize());
            while (true) {
                chunk.clear();
                if (sparse.drain(chunk) == 0) break;
                dense.addAll(chunk);
            }
        } catch (final RuntimeException e) {
            dense.release();
            throw e;
        }
        sparse.release();
        sparse = null;
        frontier = dense;
    }

    @Override
    protected void doReset() {
        if (frontier != null) frontier.release();
        startFrontier();
    }

    @Override
    protected void doClose() {
        if (frontier != null) frontier.release();
        frontier = null;
        sparse = null;
        in = null;
    }
}
