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
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrOp;
import org.apache.tinkerpop.gremlin.structure.snapshot.process.op.CsrPlan;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * The memory budget and the scratch space of one root traversal, shared by every {@code CsrSuperStep} of the traversal
 * and by every context those steps bind, including the child supersteps of standard parents that are re-bound for each
 * traverser. Sharing makes {@link MemoryBudget#limit()} a limit on the whole traversal and lets {@link #quotaFor}
 * divide it between the stateful operators that are alive at the same time.
 * <p>
 * A runtime is found through the side effects of the root traversal ({@link #acquire}), which all child traversals
 * share; the registry is weak, so a runtime that nobody forgets is collected with the traversal. Contexts
 * {@link #attach} on bind and {@link #detach} on close; when the last one detaches the scratch directory is deleted
 * (the budget is empty by then because operators release on close), and the root-level step forgets the runtime
 * when it is closed or reset. The budget peak therefore covers the whole traversal. Not thread-safe, like the rest of
 * the execution.
 */
public final class CsrRuntime {

    /**
     * The soft quota of a stateful operator is never below this many bytes (or a quarter of the budget if smaller).
     */
    static final long QUOTA_FLOOR = 16L << 10;

    private static final Map<TraversalSideEffects, CsrRuntime> RUNTIMES =
            Collections.synchronizedMap(new WeakHashMap<>());

    private final MemoryBudget budget;
    private final ScratchSpace scratch;
    private int attached;
    private int activeStateful;
    private long operatorIds;

    private CsrRuntime(final CsrSettings settings) {
        this.budget = new MemoryBudget(settings.memoryBudgetBytes());
        this.scratch = new ScratchSpace(settings.scratchDirectory());
    }

    /**
     * A runtime that no traversal shares, for plan-time use and tests.
     */
    public static CsrRuntime standalone(final CsrSettings settings) {
        return new CsrRuntime(settings);
    }

    /**
     * The runtime of the traversal whose root side effects are given, created with the settings on first use (a later
     * caller's settings are ignored).
     */
    public static CsrRuntime acquire(final TraversalSideEffects rootSideEffects, final CsrSettings settings) {
        synchronized (RUNTIMES) {
            return RUNTIMES.computeIfAbsent(rootSideEffects, k -> new CsrRuntime(settings));
        }
    }

    /**
     * Drops the runtime of the traversal if no context is attached, which resets the budget peak for its next run.
     */
    public static void forget(final TraversalSideEffects rootSideEffects) {
        synchronized (RUNTIMES) {
            final CsrRuntime runtime = RUNTIMES.get(rootSideEffects);
            if (runtime != null && runtime.attached == 0) RUNTIMES.remove(rootSideEffects);
        }
    }

    public MemoryBudget budget() {
        return budget;
    }

    public ScratchSpace scratch() {
        return scratch;
    }

    /**
     * The next operator id, unique within the runtime, see {@code AbstractCsrOperator#describe()}.
     */
    long nextOperatorId() {
        return operatorIds++;
    }

    /**
     * Registers a context whose plan has {@code stateful} stateful nodes ({@link #countStateful}) and returns its soft
     * per-operator quota in bytes: half the budget divided by the number of stateful nodes of all the plans attached now,
     * but at least {@link #QUOTA_FLOOR}. The other half is the shared
     * remainder for results, fixed reservations and buffers.
     */
    long attach(final int stateful) {
        attached++;
        activeStateful += stateful;
        final long share = budget.limit() / (2L * Math.max(1, activeStateful));
        return Math.max(share, Math.min(QUOTA_FLOOR, budget.limit() / 4));
    }

    /**
     * Undoes {@link #attach}; when no context is left the scratch directory is deleted.
     */
    void detach(final int stateful) {
        attached--;
        activeStateful -= stateful;
        if (attached == 0) scratch.close();
    }

    /**
     * The number of stateful nodes in the plan and, recursively, in the plans of its nodes.
     */
    static int countStateful(final CsrPlan plan) {
        int n = 0;
        for (final CsrOp node : plan.nodes()) {
            if (node.isStateful()) n++;
            for (final CsrPlan child : node.children()) n += countStateful(child);
        }
        return n;
    }
}
