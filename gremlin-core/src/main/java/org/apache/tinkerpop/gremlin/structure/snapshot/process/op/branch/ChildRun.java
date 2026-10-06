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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrPipeline;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

/**
 * A child plan opened as a pipeline that is fed by its parent: one entry, one batch or a list of retained batches at
 * a time. Reserves its two batches from the memory budget; the operators of the pipeline reserve their own state.
 */
final class ChildRun implements AutoCloseable {

    private final ChunkSupplier supplier = new ChunkSupplier();
    private final MemoryBudget budget;
    private final String owner;
    private final long reserved;
    private final CsrPipeline pipeline;
    private final Batch one;
    private boolean closed;

    /**
     * The batch the pipeline fills with its results, valid until the next call of {@link #next()}.
     */
    final Batch res;

    ChildRun(final CsrExecutionContext ctx, final CsrPlan plan, final String owner) {
        if (plan.inputLane() == null) throw new IllegalArgumentException("A child plan must start with Input: " + plan);
        this.budget = ctx.budget();
        this.owner = owner;
        this.one = new Batch(plan.inputLane(), 1, false);
        // a small budget shrinks the result batch instead of failing to reserve a full one for every child
        this.res = new Batch(plan.outputLane(), Batch.capacityWithin(plan.outputLane(), plan.outputRecordsSource(),
                ctx.batchSize(), ctx.budget().available() / 8), plan.outputRecordsSource());
        this.reserved = one.estimatedBytes() + res.estimatedBytes();
        budget.reserve(reserved, owner);
        CsrPipeline opened = null;
        try {
            opened = CsrPipeline.open(ctx, plan, supplier);
        } finally {
            if (opened == null) budget.release(reserved, owner);
        }
        this.pipeline = opened;
    }

    /**
     * Starts a run over entry {@code i} of the batch with the given bulk. Any run in progress is abandoned.
     */
    void begin(final Batch source, final int i, final long bulk) {
        one.clear();
        one.copyEntry(source, i, bulk);
        supplier.set(one);
        pipeline.reset();
    }

    /**
     * Starts a run over all entries of the batch. The pipeline is reset first.
     */
    void beginBatch(final Batch source) {
        supplier.set(source);
        pipeline.reset();
    }

    /**
     * Starts a run over the entries the supplier delivers, which the pipeline sees as one stream.
     */
    void beginReplay(final BatchSupplier source) {
        supplier.setReplay(source);
        pipeline.reset();
    }

    /**
     * Pulls the next results into {@link #res}.
     *
     * @return false when the run is exhausted
     */
    boolean next() {
        return pipeline.next(res);
    }

    /**
     * Whether the run that was just begun produces a result. Stops at the first.
     */
    boolean exists(final Batch source, final int i, final long bulk) {
        begin(source, i, bulk);
        return pipeline.next(res);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            pipeline.close();
        } finally {
            budget.release(reserved, owner);
        }
    }
}
