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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Batch;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.CsrExecutionContext;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.Lane;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.Ops;

/**
 * Decides for one entry whether a child plan has a result, with the child fed bulk 1 like {@code TraversalUtil.test}.
 * <p>
 * A child that is one adjacency step from a vertex ({@code where(outE('x'))}, {@code filter(out())}) needs no pipeline:
 * the answer is in the adjacency. Without labels that is an offset comparison; with labels a semi-join bitset over the
 * vertices is built in one pass over the edges once enough vertices were tested to pay for it. Every other child runs as
 * a pipeline that stops at its first result, and for vertex and edge inputs the outcome is memoized per ordinal in
 * bitsets, unless the child writes a side effect.
 */
final class ExistenceTest implements AutoCloseable {

    private final CsrExecutionContext ctx;
    private final String owner;
    private final ChildRun run;
    private final OrdinalBits memo;

    // adjacency probe
    private final Direction direction;
    private final boolean[] labelFlags;
    private final boolean nothing;
    private CsrSnapshot snapshot;
    private long[] semi;
    private long semiBytes;
    private boolean semiFailed;
    private long tests;

    private ExistenceTest(final CsrExecutionContext ctx, final String owner, final ChildRun run,
                          final OrdinalBits memo, final Ops.Expand expand) {
        this.ctx = ctx;
        this.owner = owner;
        this.run = run;
        this.memo = memo;
        if (expand == null) {
            direction = null;
            labelFlags = null;
            nothing = false;
        } else {
            snapshot = ctx.snapshot();
            direction = expand.direction();
            final int[] codes = expand.labelCodes();
            nothing = codes != null && codes.length == 0;
            if (codes != null && codes.length > 0) {
                labelFlags = new boolean[snapshot.edgeLabels().size()];
                for (final int code : codes) {
                    if (code >= 0 && code < labelFlags.length) labelFlags[code] = true;
                }
            } else {
                labelFlags = null;
            }
        }
    }

    static ExistenceTest create(final CsrExecutionContext ctx, final CsrPlan plan, final String owner) {
        if (plan.nodes().size() == 2 && plan.inputLane() == Lane.V && plan.nodes().get(1) instanceof Ops.Expand) {
            return new ExistenceTest(ctx, owner, null, null, (Ops.Expand) plan.nodes().get(1));
        }
        final OrdinalBits memo = Plans.writesSideEffects(plan) ? null
                : OrdinalBits.tryCreate(ctx, Plans.universe(ctx.snapshot(), plan.inputLane()), owner + " memo");
        try {
            return new ExistenceTest(ctx, owner, new ChildRun(ctx, plan, owner + " child"), memo, null);
        } catch (RuntimeException e) {
            if (memo != null) memo.close();
            throw e;
        }
    }

    /**
     * Whether the child has a result for entry {@code i} of the batch.
     */
    boolean test(final Batch in, final int i) {
        if (direction != null) return probe(in.ord[i]);
        if (memo != null) {
            final int ordinal = in.ord[i];
            final int known = memo.get(ordinal);
            if (known >= 0) return known == 1;
            final boolean outcome = run.exists(in, i, 1L);
            memo.put(ordinal, outcome);
            return outcome;
        }
        return run.exists(in, i, 1L);
    }

    private boolean probe(final int vertex) {
        if (nothing) return false;
        if (semi != null) return (semi[vertex >>> 6] & (1L << vertex)) != 0;
        if (labelFlags == null) {
            switch (direction) {
                case OUT:
                    return snapshot.outEnd(vertex) > snapshot.outStart(vertex);
                case IN:
                    return snapshot.inEnd(vertex) > snapshot.inStart(vertex);
                default:
                    return snapshot.outEnd(vertex) > snapshot.outStart(vertex)
                            || snapshot.inEnd(vertex) > snapshot.inStart(vertex);
            }
        }
        if (!semiFailed && ++tests * 8 > snapshot.vertexCount() && buildSemiJoin()) {
            return (semi[vertex >>> 6] & (1L << vertex)) != 0;
        }
        if (direction != Direction.IN) {
            for (long p = snapshot.outStart(vertex), end = snapshot.outEnd(vertex); p < end; p++) {
                if (matches(snapshot.outEdge(p))) return true;
            }
        }
        if (direction != Direction.OUT) {
            for (long p = snapshot.inStart(vertex), end = snapshot.inEnd(vertex); p < end; p++) {
                if (matches(snapshot.inEdge(p))) return true;
            }
        }
        return false;
    }

    private boolean matches(final int edge) {
        final int code = snapshot.edgeLabelCode(edge);
        return code >= 0 && code < labelFlags.length && labelFlags[code];
    }

    /**
     * Sets a bit for every vertex that has an edge of the labels in the direction, in one pass over the edges.
     */
    private boolean buildSemiJoin() {
        final MemoryBudget budget = ctx.budget();
        final int vertices = snapshot.vertexCount();
        final long bytes = 8L * ((vertices + 63L) >>> 6);
        if (!budget.tryReserve(bytes, owner + " semi-join")) {
            semiFailed = true;
            return false;
        }
        semiBytes = bytes;
        final long[] bits = new long[(int) (bytes >>> 3)];
        final int edges = snapshot.edgeCount();
        for (int e = 0; e < edges; e++) {
            if ((e & 0xFFFF) == 0) ctx.checkInterrupt();
            if (!matches(e)) continue;
            if (direction != Direction.IN) {
                final int v = snapshot.edgeOut(e);
                bits[v >>> 6] |= 1L << v;
            }
            if (direction != Direction.OUT) {
                final int v = snapshot.edgeIn(e);
                bits[v >>> 6] |= 1L << v;
            }
        }
        semi = bits;
        return true;
    }

    /**
     * Forgets the memo, which may hold outcomes computed with values a reset re-reads.
     */
    void reset() {
        if (memo != null) memo.clear();
        tests = 0;
    }

    @Override
    public void close() {
        try {
            if (run != null) run.close();
        } finally {
            if (memo != null) memo.close();
            if (semiBytes > 0) {
                ctx.budget().release(semiBytes, owner + " semi-join");
                semiBytes = 0;
            }
            semi = null;
        }
    }
}
