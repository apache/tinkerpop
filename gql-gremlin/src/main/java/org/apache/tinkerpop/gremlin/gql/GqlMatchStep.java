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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;



import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * TinkerGraph-specific executable replacement for the generic {@link DeclarativeMatchStep}.
 *
 * <p>The planner and executor are normally injected by {@code TinkerGraphDeclarativeMatchStrategy}
 * as graph-level singletons so the compiled-plan cache is shared across all traversals on the
 * same graph instance. When the step is constructed without injected instances (e.g. in tests
 * using a substitute strategy), they are initialised lazily from the traversal's graph on the
 * first call to {@link #processNextStart()}.</p>
 *
 * <p>For each incoming traverser the plan is executed against the graph and one output traverser
 * is emitted per result row. Each result row's named variable bindings are recorded in the
 * emitted traverser's path so that downstream {@code select()} steps can retrieve them by
 * name.</p>
 *
 * <p>Only the {@code "gql"} query language is supported. If the step's query language is set
 * to any other non-null value, {@link UnsupportedOperationException} is thrown.</p>
 *
 * @param <S> the traverser start type
 */
public final class GqlMatchStep<S> extends DeclarativeMatchStep<S> {

    /**
     * The query language identifier that this step accepts when set explicitly.
     * {@code null} is also accepted and is treated as a request to use this native language.
     */
    public static final String SUPPORTED_QUERY_LANGUAGE = "gql";

    private GqlPlanner planner;
    private GqlExecutor executor;
    private GqlMatchPlan plan;
    // Lazy row source: one iterator per spawn execution or per incoming mid-traversal traverser.
    private Iterator<Element[]> rowIterator = null;
    // For mid-traversal: the upstream traverser whose rows rowIterator is currently serving.
    private Traverser.Admin<S> currentStart = null;
    private boolean done = false;

    /**
     * Constructs a {@code TinkerGraphMatchStep} without pre-injected planner or executor.
     * Both will be initialised lazily from the traversal's graph on the first
     * {@link #processNextStart()} call. Intended for use in tests where a substitute strategy
     * replaces the placeholder step without access to graph-level singletons.
     *
     * @param originalStep the step being replaced by this concrete implementation
     */
    public GqlMatchStep(final DeclarativeMatchStep<S> originalStep) {
        this(originalStep, null, null);
    }

    /**
     * Constructs a {@code TinkerGraphMatchStep} with pre-injected graph-level singletons.
     * When {@code planner} and {@code executor} are non-null the plan cache is shared across
     * all traversals on the same graph instance. Called by
     * {@code TinkerGraphDeclarativeMatchStrategy}.
     *
     * @param originalStep the step being replaced
     * @param planner      the graph-level planner singleton, or {@code null} for lazy init
     * @param executor     the graph-level executor singleton, or {@code null} for lazy init
     */
    public GqlMatchStep(final DeclarativeMatchStep<S> originalStep,
                                final GqlPlanner planner,
                                final GqlExecutor executor) {
        super(originalStep.getTraversal(), originalStep.getMatchQuery(),
              originalStep.getParams(), originalStep.getQueryLanguage(), originalStep.isStart());
        originalStep.getLabels().forEach(this::addLabel);
        this.planner = planner;
        this.executor = executor;
    }

    /**
     * Returns the next output traverser.
     *
     * <p>On the first invocation (or after a {@link #clone()}), if the planner was not
     * injected it is initialised from the traversal's graph. The compiled {@link GqlMatchPlan}
     * is always obtained from the planner's cache so identical query strings cost only a map
     * lookup after the first compilation.</p>
     *
     * @throws UnsupportedOperationException if the query language is non-null and not "gql"
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Traverser.Admin<Map<String, Object>> processNextStart() throws NoSuchElementException {
        final String ql = getQueryLanguage();
        if (ql != null && !SUPPORTED_QUERY_LANGUAGE.equals(ql)) {
            throw new UnsupportedOperationException(
                    "TinkerGraphMatchStep only supports query language '" + SUPPORTED_QUERY_LANGUAGE +
                    "', got: " + ql);
        }

        final java.util.Map<String, Object> params = getParams();
        final java.util.Map<String, Object> resolvedParams =
                (params != null && !params.isEmpty()) ? params : java.util.Collections.emptyMap();

        // Ensure planner and executor are available. When injected by the strategy they are
        // graph-level singletons; otherwise fall back to per-step lazy initialisation.
        if (planner == null) {
            final Graph graph = this.getTraversal().getGraph().get();
            planner = new DefaultGqlPlanner(graph);
            executor = new DefaultGqlExecutor(graph);
        }
        if (plan == null) {
            plan = planner.plan(getMatchQuery());
        }

        if (isStart()) {
            // Spawn path: no upstream traversers. Pull rows lazily from the row iterator —
            // one row per processNextStart() call — so downstream limit() pays only for what
            // it consumes.
            if (!done) {
                if (rowIterator == null)
                    rowIterator = executor.execute(plan, resolvedParams);
                if (rowIterator.hasNext())
                    return rowToSpawnTraverser(rowIterator.next(), plan);
                done = true;
            }
            throw FastNoSuchElementException.instance();
        }

        // Mid-traversal path: pull rows lazily per incoming traverser.
        while (true) {
            if (rowIterator != null && rowIterator.hasNext())
                return rowToSplitTraverser(rowIterator.next(), plan, currentStart);

            // Current row iterator exhausted — advance to the next incoming traverser.
            if (!this.starts.hasNext()) throw FastNoSuchElementException.instance();
            currentStart = this.starts.next();
            rowIterator = executor.execute(plan, resolvedParams);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Traverser.Admin<Map<String, Object>> rowToSpawnTraverser(final Element[] row, final GqlMatchPlan plan) {
        final Traverser.Admin<Map<String, Object>> traverser =
                this.getTraversal().getTraverserGenerator().generate(Collections.emptyMap(), (Step) this, 1L);
        bindRow(row, plan, (Traverser.Admin) traverser);
        return traverser;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Traverser.Admin<Map<String, Object>> rowToSplitTraverser(final Element[] row, final GqlMatchPlan plan,
                                                                      final Traverser.Admin<S> start) {
        final Traverser.Admin<Map<String, Object>> split =
                (Traverser.Admin<Map<String, Object>>) start.split(Collections.emptyMap(), (Step) this);
        bindRow(row, plan, (Traverser.Admin) split);
        return split;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void bindRow(final Element[] row, final GqlMatchPlan plan,
                         final Traverser.Admin traverser) {
        final String[] variables = plan.getVariables();
        final Map<String, Object> bindings = new LinkedHashMap<>();
        for (int i = 0; i < variables.length; i++) {
            final String var = variables[i];
            if (!var.startsWith("$anon") && row[i] != null) {
                traverser.set(row[i]);
                traverser.addLabels(Collections.singleton(var));
                bindings.put(var, row[i]);
            }
        }
        traverser.set(bindings);
    }

    /**
     * Declares {@link TraverserRequirement#LABELED_PATH} so that traversers track their
     * path labels, enabling downstream {@code select()} steps to retrieve bound variables.
     */
    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.LABELED_PATH);
    }

    /**
     * Creates a clone with a fresh output queue and a null plan reference so the plan is
     * re-fetched from the planner's cache on first use. The planner and executor references
     * are retained: when they are graph-level singletons (the normal path) the clone shares
     * the plan cache; when they are per-step instances (test path) the clone will lazily
     * re-create its own.
     */
    @Override
    @SuppressWarnings("unchecked")
    public GqlMatchStep<S> clone() {
        final GqlMatchStep<S> clone = (GqlMatchStep<S>) super.clone();
        clone.rowIterator = null;
        clone.currentStart = null;
        clone.plan = null;
        clone.done = false;
        return clone;
    }

    /**
     * Resets lazy row state. The planner and executor singletons are preserved so the
     * QueryGraph parse cache survives a traversal reset, but the compiled {@link GqlMatchPlan}
     * is cleared so that seed selection is recomputed from live label counts on the next
     * execution — ensuring graph mutations between resets are reflected.
     */
    @Override
    public void reset() {
        super.reset();
        rowIterator = null;
        currentStart = null;
        plan = null;
        done = false;
    }
}
