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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.graph.CsrGraph;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The per-execution state shared by the operators of one {@code CsrSuperStep}: the graph and snapshot, the memory
 * budget, the scratch space, the operator factory, the side effects, interrupt polling and the registry of columns that
 * lazy {@code VAL} entries refer to. It is created when the step binds (on the first {@code processNextStart()}) and
 * closed on reset and close, which releases scratch files. Child pipelines share the context of their parent.
 * Instances are not thread-safe.
 */
public final class CsrExecutionContext implements AutoCloseable {

    private final CsrGraph graph;
    private final CsrSnapshot snapshot;
    private final CsrSettings settings;
    private final CsrRuntime runtime;
    private final MemoryBudget budget;
    private final ScratchSpace scratch;
    private final long quota;
    private final int stateful;
    private boolean closed;
    private final CsrOperatorFactory factory;
    private final TraversalSideEffects sideEffects;
    private final String stepName;
    private final List<ColumnReader> columns = new ArrayList<>();
    private final Map<ColumnReader, Integer> columnIds = new IdentityHashMap<>();

    /**
     * @param sideEffects the side effects of the traversal, or null when there is none (plan-time use)
     * @param stepName    the name of the step for messages
     */
    public CsrExecutionContext(final CsrGraph graph, final CsrSettings settings, final CsrOperatorFactory factory,
                               final TraversalSideEffects sideEffects, final String stepName) {
        this(graph, settings, factory, sideEffects, stepName, CsrRuntime.standalone(settings), null);
    }

    /**
     * @param runtime the budget and scratch of the root traversal, shared with the other contexts of the traversal
     * @param plan    the plan this context runs, which sizes the operator quota; null counts no stateful operators
     */
    public CsrExecutionContext(final CsrGraph graph, final CsrSettings settings, final CsrOperatorFactory factory,
                               final TraversalSideEffects sideEffects, final String stepName, final CsrRuntime runtime,
                               final CsrPlan plan) {
        this.graph = Objects.requireNonNull(graph);
        this.snapshot = graph.snapshot();
        this.settings = Objects.requireNonNull(settings);
        this.runtime = Objects.requireNonNull(runtime);
        this.budget = runtime.budget();
        this.scratch = runtime.scratch();
        this.stateful = plan == null ? 0 : CsrRuntime.countStateful(plan);
        this.quota = runtime.attach(stateful);
        this.factory = Objects.requireNonNull(factory);
        this.sideEffects = sideEffects;
        this.stepName = stepName;
    }

    public CsrGraph graph() {
        return graph;
    }

    public CsrSnapshot snapshot() {
        return snapshot;
    }

    public CsrSettings settings() {
        return settings;
    }

    /**
     * The batch capacity, {@code settings().batchSize()}.
     */
    public int batchSize() {
        return settings.batchSize();
    }

    public MemoryBudget budget() {
        return budget;
    }

    /**
     * The scratch space of the root traversal, shared by all its contexts. Operators discard what they create; what is
     * left is deleted when the last context of the traversal closes.
     */
    public ScratchSpace scratch() {
        return scratch;
    }

    /**
     * The soft quota in bytes for the spillable state of one operator, fixed when the context is created: half the
     * budget divided by the stateful operators alive in the traversal (see {@link CsrRuntime#attach}). Spillable state
     * reserves with {@link #tryReserveWithinQuota} and spills when that fails; state that cannot be spilled (results)
     * reserves from the budget directly and so draws from the shared remainder.
     */
    public long quota() {
        return quota;
    }

    /**
     * {@link MemoryBudget#tryReserveWithin} with this context's quota: the quota applies to each owner string, so an
     * operator uses one owner for each kind of spillable state.
     */
    public boolean tryReserveWithinQuota(final long bytes, final String owner) {
        return budget.tryReserveWithin(bytes, owner, quota);
    }

    /**
     * The next operator id, unique in the traversal, for owner strings.
     */
    long nextOperatorId() {
        return runtime.nextOperatorId();
    }

    public CsrOperatorFactory factory() {
        return factory;
    }

    /**
     * The side effects of the traversal, null when the context was created without them.
     */
    public TraversalSideEffects sideEffects() {
        return sideEffects;
    }

    /**
     * The name of the step this execution belongs to, for exception messages.
     */
    public String stepName() {
        return stepName;
    }

    /**
     * Throws if the thread was interrupted, like {@code AbstractStep} does. {@link AbstractCsrOperator} calls this once
     * per batch; long-running loops inside an operator should call it too.
     *
     * @throws TraversalInterruptedException if the thread was interrupted
     */
    public void checkInterrupt() {
        if (Thread.interrupted()) throw new TraversalInterruptedException();
    }

    /**
     * Registers a column so that {@code VAL} entries can refer to it by id, see {@code Batch.addColumnValue}. Registering
     * the same reader again returns the same id.
     */
    public int registerColumn(final ColumnReader column) {
        return columnIds.computeIfAbsent(column, c -> {
            columns.add(c);
            return columns.size() - 1;
        });
    }

    public ColumnReader column(final int columnId) {
        return columns.get(columnId);
    }

    /**
     * Decodes a column reference: the value of entry {@code entryIndex} of the registered column, null for a null value.
     */
    public Object columnValue(final int columnId, final long entryIndex) {
        return columns.get(columnId).getAt(entryIndex);
    }

    /**
     * Detaches from the traversal's runtime, which deletes the scratch files when it was the last context. Budget
     * reservations are released by the operators that made them. Idempotent.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        runtime.detach(stateful);
    }
}
