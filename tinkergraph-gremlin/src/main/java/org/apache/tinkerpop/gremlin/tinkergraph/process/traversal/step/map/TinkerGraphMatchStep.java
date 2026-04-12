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
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

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
 * <p>On the first call to {@link #processNextStart()}, the GQL query is compiled into a
 * {@link GqlMatchPlan} via {@link TinkerGraphGqlPlanner} (which caches the plan internally).
 * For each incoming traverser the plan is executed against the graph and one output traverser
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

    private final ArrayDeque<Traverser.Admin<Optional>> outputQueue = new ArrayDeque<>();

    private TinkerGraphGqlPlanner planner;
    private TinkerGraphGqlExecutor executor;
    private GqlMatchPlan plan;

    /**
     * Constructs a {@code TinkerGraphMatchStep} by copying all state from the
     * {@link DeclarativeMatchStep} placeholder that this step replaces.
     *
     * @param originalStep the step being replaced by this concrete implementation
     */
    public TinkerGraphMatchStep(final DeclarativeMatchStep<S> originalStep) {
        super(originalStep.getTraversal(), originalStep.getGqlQuery(),
              originalStep.getParams(), originalStep.getQueryLanguage());
        originalStep.getLabels().forEach(this::addLabel);
    }

    /**
     * Returns the next output traverser.
     *
     * <p>On the first invocation, the planner and executor are lazily initialised using the
     * {@link TinkerGraph} obtained from the traversal. The compiled {@link GqlMatchPlan} is
     * cached for the lifetime of this step instance.</p>
     *
     * <p>For each input traverser, {@link TinkerGraphGqlExecutor#execute(GqlMatchPlan)} is
     * called to obtain result rows. One output traverser is emitted per row: the traverser is
     * created via {@code start.split(Optional.empty(), this)} and each named binding
     * ({@code varName → element}) is appended to its path.</p>
     *
     * @throws UnsupportedOperationException if the query language is non-null and not "gql"
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

        if (!outputQueue.isEmpty()) {
            return outputQueue.poll();
        }

        // Lazy initialisation: compile the plan once, on the first processNextStart() call.
        if (planner == null) {
            final TinkerGraph graph = (TinkerGraph) this.getTraversal().getGraph().get();
            planner = new TinkerGraphGqlPlanner(graph);
            executor = new TinkerGraphGqlExecutor(graph);
            plan = planner.plan(getGqlQuery());
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
     * Clears the output queue on reset. The compiled plan is intentionally preserved so
     * the planner cache survives a traversal reset.
     */
    @Override
    public void reset() {
        super.reset();
        outputQueue.clear();
    }
}
