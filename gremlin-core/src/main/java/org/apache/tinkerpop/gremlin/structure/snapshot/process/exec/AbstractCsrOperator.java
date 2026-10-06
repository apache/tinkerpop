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
package org.apache.tinkerpop.gremlin.structure.snapshot.process.exec;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;

import java.util.Objects;

/**
 * The base class of every operator. It implements the {@link CsrOperator} lifecycle, interrupt polling, statistics and
 * timing, and leaves four hooks to subclasses:
 * <ul>
 *     <li>{@link #doOpen()}: allocate state; {@link #ctx} is set;</li>
 *     <li>{@link #produce(Batch)}: fill the output batch, see below;</li>
 *     <li>{@link #doReset()}: return to the opened state;</li>
 *     <li>{@link #doClose()}: release everything, including {@code ctx.budget()} reservations.</li>
 * </ul>
 * {@code produce} appends entries to {@code out}, which is empty and of the operator's output shape, and returns true
 * if the operator may have more output, false when it is exhausted. Returning true with an empty batch is allowed (for
 * example a filter that rejected a whole input batch) and makes the base class call {@code produce} again, so
 * {@code produce} never has to loop to find a non-empty result. {@code produce} must stop appending when the batch is
 * full and resume on the next call, keeping its own cursor. A source operator has no upstream and generates entries;
 * other operators call {@link #pull(Batch)} for their input. Memory that lives for the whole execution is reserved in
 * {@code doOpen} or lazily, under an owner string made with {@link #owner(String)}.
 */
public abstract class AbstractCsrOperator implements CsrOperator {

    protected final OperatorSpec spec;
    protected CsrExecutionContext ctx;
    private final OperatorStats stats = new OperatorStats();
    private long upstreamNanos;
    private long id = -1;
    private boolean open;
    private boolean exhausted;

    protected AbstractCsrOperator(final OperatorSpec spec) {
        this.spec = Objects.requireNonNull(spec);
    }

    @Override
    public final CsrOp node() {
        return spec.node();
    }

    @Override
    public final Lane outputLane() {
        return spec.outputLane();
    }

    @Override
    public final boolean outputRecordsSource() {
        return spec.outputRecordsSource();
    }

    @Override
    public final Batch newOutputBatch(final int capacity) {
        return spec.newOutputBatch(capacity);
    }

    @Override
    public final OperatorStats stats() {
        return stats;
    }

    @Override
    public final void open(final CsrExecutionContext context) {
        if (open) throw new IllegalStateException(describe() + " is already open");
        this.ctx = Objects.requireNonNull(context);
        if (id < 0) id = context.nextOperatorId();
        stats.clear();
        doOpen();
        open = true;
    }

    @Override
    public final boolean next(final Batch out) {
        if (!open) throw new IllegalStateException(describe() + " is not open");
        if (out.lane != spec.outputLane() || out.recordSource != spec.outputRecordsSource()) {
            throw new IllegalArgumentException(describe() + " emits " + spec.outputLane()
                    + (spec.outputRecordsSource() ? " with sources" : "") + " but was given a batch of " + out.lane
                    + (out.recordSource ? " with sources" : ""));
        }
        final long start = System.nanoTime();
        upstreamNanos = 0;
        try {
            out.clear();
            while (!exhausted) {
                ctx.checkInterrupt();
                if (!produce(out)) exhausted = true;
                if (out.n > 0) {
                    stats.addOut(out.n, out.totalBulk());
                    return true;
                }
            }
            return false;
        } finally {
            stats.addNanos(System.nanoTime() - start - upstreamNanos);
        }
    }

    /**
     * Pulls the next batch from the upstream operator, counting its entries and excluding its time from this operator's.
     *
     * @param in a batch of the upstream output shape, see {@link OperatorSpec#newInputBatch(int)}
     * @return false when upstream is exhausted
     */
    protected final boolean pull(final Batch in) {
        final long start = System.nanoTime();
        final boolean has = spec.upstream().next(in);
        upstreamNanos += System.nanoTime() - start;
        if (has) stats.addIn(in.n);
        return has;
    }

    @Override
    public final void reset() {
        if (!open) return;
        exhausted = false;
        doReset();
    }

    @Override
    public final void close() {
        if (!open) return;
        open = false;
        try {
            doClose();
        } finally {
            ctx = null;
        }
    }

    protected abstract void doOpen();

    protected abstract boolean produce(Batch out);

    protected abstract void doReset();

    protected abstract void doClose();

    /**
     * The owner string for budget reservations and messages, {@code "<node name>#<id> <state>"}. The id is unique in the
     * traversal (the budget is shared by all its contexts), so {@code releaseAll(owner(state))} never touches another
     * operator.
     */
    protected final String owner(final String state) {
        return describe() + " " + state;
    }

    /**
     * {@code "<node name>#<id>"}, with the plan index until the operator is opened.
     */
    protected final String describe() {
        return spec.node().name() + "#" + (id >= 0 ? id : spec.index());
    }
}
