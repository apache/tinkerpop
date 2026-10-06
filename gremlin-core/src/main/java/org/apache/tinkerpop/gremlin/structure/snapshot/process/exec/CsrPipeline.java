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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Sources;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link CsrPlan} turned into a chain of opened operators. The top-level pipeline of a {@code CsrSuperStep} and the
 * child pipelines that branching operators run per input are the same class; a child shares its parent's
 * {@link CsrExecutionContext}. Create a child pipeline from an operator's {@code doOpen} and close it from
 * {@code doClose}.
 */
public final class CsrPipeline implements AutoCloseable {

    private final CsrPlan plan;
    private final List<CsrOperator> operators;
    private boolean closed;

    private CsrPipeline(final CsrPlan plan, final List<CsrOperator> operators) {
        this.plan = plan;
        this.operators = Collections.unmodifiableList(operators);
    }

    /**
     * Builds and opens the operators of the plan with the context's factory. If any fails to open, the ones already
     * opened are closed and the failure is rethrown.
     *
     * @param input feeds the plan's {@code Input} source; must be null if the plan does not start with one
     */
    public static CsrPipeline open(final CsrExecutionContext ctx, final CsrPlan plan, final BatchSupplier input) {
        Objects.requireNonNull(ctx);
        Objects.requireNonNull(plan);
        if ((plan.inputLane() != null) != (input != null)) {
            throw new IllegalArgumentException("A BatchSupplier is needed exactly when the plan starts with Input");
        }
        final List<CsrOperator> operators = new ArrayList<>();
        CsrOperator previous = null;
        Lane lane = null;
        boolean recordsSource = false;
        for (int i = 0; i < plan.nodes().size(); i++) {
            final CsrOp node = plan.nodes().get(i);
            final Lane outLane = plan.laneAfter(i);
            final boolean outSource = plan.recordsSourceAfter(i);
            final OperatorSpec spec = new OperatorSpec(node, i, lane, recordsSource, outLane, outSource, previous,
                    i == 0 ? input : null);
            final CsrOperator operator = ctx.factory().create(spec);
            operators.add(operator);
            previous = operator;
            lane = outLane;
            recordsSource = outSource;
        }
        int opened = 0;
        try {
            for (final CsrOperator operator : operators) {
                operator.open(ctx);
                opened++;
            }
        } catch (RuntimeException e) {
            for (int i = opened - 1; i >= 0; i--) {
                try {
                    operators.get(i).close();
                } catch (RuntimeException suppressed) {
                    e.addSuppressed(suppressed);
                }
            }
            throw e;
        }
        return new CsrPipeline(plan, operators);
    }

    public CsrPlan plan() {
        return plan;
    }

    /**
     * The operators, source first.
     */
    public List<CsrOperator> operators() {
        return operators;
    }

    private CsrOperator last() {
        return operators.get(operators.size() - 1);
    }

    public Lane outputLane() {
        return last().outputLane();
    }

    /**
     * A new empty batch of the shape {@link #next(Batch)} expects.
     */
    public Batch newOutputBatch(final int capacity) {
        return last().newOutputBatch(capacity);
    }

    /**
     * Fills {@code out} with the next result entries, see {@link CsrOperator#next(Batch)}.
     */
    public boolean next(final Batch out) {
        return last().next(out);
    }

    /**
     * Resets every operator, source first, so the pipeline can run again over new input.
     */
    public void reset() {
        for (final CsrOperator operator : operators) operator.reset();
    }

    /**
     * Closes every operator, last first. Idempotent.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        for (int i = operators.size() - 1; i >= 0; i--) {
            try {
                operators.get(i).close();
            } catch (RuntimeException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            }
        }
        if (failure != null) throw failure;
    }
}
