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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.tinkergraph.process.gql.GqlMatchPlan;
import org.apache.tinkerpop.gremlin.tinkergraph.process.gql.TinkerGraphGqlExecutor;
import org.apache.tinkerpop.gremlin.tinkergraph.process.gql.TinkerGraphGqlPlanner;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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
public final class TinkerGraphMatchStep<S> extends DeclarativeMatchStep<S> {

    private ArrayDeque<Traverser.Admin<Optional>> outputQueue = new ArrayDeque<>();

    private TinkerGraphGqlPlanner planner;
    private TinkerGraphGqlExecutor executor;
    private GqlMatchPlan plan;
    private boolean done = false;

    /**
     * Constructs a {@code TinkerGraphMatchStep} without pre-injected planner or executor.
     * Both will be initialised lazily from the traversal's graph on the first
     * {@link #processNextStart()} call. Intended for use in tests where a substitute strategy
     * replaces the placeholder step without access to graph-level singletons.
     *
     * @param originalStep the step being replaced by this concrete implementation
     */
    public TinkerGraphMatchStep(final DeclarativeMatchStep<S> originalStep) {
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
    public TinkerGraphMatchStep(final DeclarativeMatchStep<S> originalStep,
                                final TinkerGraphGqlPlanner planner,
                                final TinkerGraphGqlExecutor executor) {
        super(originalStep.getTraversal(), originalStep.getGqlQuery(),
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
     * @throws UnsupportedOperationException if the query language is non-null and not "gql",
     *                                       or if non-empty query parameters are supplied
     */
    @Override
    @SuppressWarnings("unchecked")
    protected Traverser.Admin<Optional> processNextStart() throws NoSuchElementException {
        final String ql = getQueryLanguage();
        if (ql != null && !DEFAULT_QUERY_LANGUAGE.equals(ql)) {
            throw new UnsupportedOperationException(
                    "TinkerGraphMatchStep only supports query language '" + DEFAULT_QUERY_LANGUAGE +
                    "', got: " + ql);
        }

        final java.util.Map<String, Object> params = getParams();
        if (params != null && !params.isEmpty()) {
            throw new UnsupportedOperationException(
                    "TinkerGraphMatchStep does not yet support query parameters — " +
                    "pass null or an empty map until parameter support is implemented");
        }

        if (!outputQueue.isEmpty()) {
            return outputQueue.poll();
        }

        // Ensure planner and executor are available. When injected by the strategy they are
        // graph-level singletons; otherwise fall back to per-step lazy initialisation.
        if (planner == null) {
            final AbstractTinkerGraph graph = (AbstractTinkerGraph) this.getTraversal().getGraph().get();
            planner = new TinkerGraphGqlPlanner(graph);
            executor = new TinkerGraphGqlExecutor(graph);
        }
        if (plan == null) {
            plan = planner.plan(getGqlQuery());
        }

        if (isStart()) {
            // Source-spawn path: no upstream traversers — execute the plan once and generate
            // a fresh traverser for each result row using the traversal's TraverserGenerator.
            if (!done) {
                done = true;
                final List<Map<String, Element>> rows = executor.execute(plan);
                for (final Map<String, Element> row : rows) {
                    final Traverser.Admin<Optional> traverser =
                            this.getTraversal().getTraverserGenerator().generate(Optional.empty(), (Step) this, 1L);
                    for (final Map.Entry<String, Element> entry : row.entrySet()) {
                        if (!entry.getKey().startsWith("$anon")) {
                            ((Traverser.Admin) traverser).set(entry.getValue());
                            traverser.addLabels(Collections.singleton(entry.getKey()));
                        }
                    }
                    ((Traverser.Admin) traverser).set(Optional.empty());
                    outputQueue.add(traverser);
                }
            }
            if (!outputQueue.isEmpty()) return outputQueue.poll();
            throw FastNoSuchElementException.instance();
        }

        while (this.starts.hasNext()) {
            final Traverser.Admin<S> start = this.starts.next();
            final List<Map<String, Element>> rows = executor.execute(plan);

            for (final Map<String, Element> row : rows) {
                @SuppressWarnings("unchecked")
                final Traverser.Admin<Optional> split =
                        (Traverser.Admin<Optional>) start.split(Optional.empty(), (Step) this);
                for (final Map.Entry<String, Element> entry : row.entrySet()) {
                    if (!entry.getKey().startsWith("$anon")) {
                        // Temporarily set the traverser value to the bound element so that
                        // addLabels() records it as a new labeled path entry, then restore
                        // the traverser value to Optional.empty() once all bindings are added.
                        ((Traverser.Admin) split).set(entry.getValue());
                        split.addLabels(Collections.singleton(entry.getKey()));
                    }
                }
                ((Traverser.Admin) split).set(Optional.empty());
                outputQueue.add(split);
            }

            if (!outputQueue.isEmpty()) {
                return outputQueue.poll();
            }
        }

        throw FastNoSuchElementException.instance();
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
    public TinkerGraphMatchStep<S> clone() {
        final TinkerGraphMatchStep<S> clone = (TinkerGraphMatchStep<S>) super.clone();
        clone.outputQueue = new ArrayDeque<>();
        clone.plan = null;
        clone.done = false;
        return clone;
    }

    /**
     * Clears the output queue on reset. The planner, executor, and compiled plan are
     * preserved so the plan cache survives a traversal reset.
     */
    @Override
    public void reset() {
        super.reset();
        outputQueue.clear();
        done = false;
    }
}
