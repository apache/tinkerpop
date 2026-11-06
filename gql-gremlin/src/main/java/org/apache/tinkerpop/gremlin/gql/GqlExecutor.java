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

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;
import java.util.Map;

/**
 * Executes a {@link GqlMatchPlan} against a graph and returns result rows.
 *
 * <p>Each result row is an {@code Element[]} whose indices correspond to the variable indices
 * defined in the plan; use {@link GqlMatchPlan#getVariables()} to map indices back to variable
 * names. The seed variable is always at index 0.</p>
 *
 * <p>{@link DefaultGqlExecutor} is the reference implementation. It iterates seed vertices via
 * a full scan or an index lookup when
 * {@link org.apache.tinkerpop.gremlin.structure.Graph.Index} is available, then performs DFS
 * backtracking over the plan's extension steps using the TinkerPop
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} API ({@code Vertex.edges()},
 * {@code Edge.inVertex()}, etc.).</p>
 *
 * <h3>When to implement this interface</h3>
 *
 * <p>This is the primary customization point for providers with a native query or traversal
 * engine. Such a provider can keep {@link DefaultGqlPlanner} — getting free GQL MATCH parsing
 * and cardinality-guided join ordering driven by the {@code countVerticesByLabel},
 * {@code countEdgesByLabel}, and {@link org.apache.tinkerpop.gremlin.structure.Graph.Index}
 * overrides already on the graph — and replace only the executor to translate the resulting
 * {@link GqlMatchPlan} into native operations instead of iterating {@code Graph.vertices()}.</p>
 *
 * <p>The contract for implementors:</p>
 * <ul>
 *   <li>Iterate seed vertex candidates matching the plan's seed label and predicates.</li>
 *   <li>For each seed, extend the partial match through the plan's
 *       {@link GqlMatchPlan#getSteps() extension steps} in an order that keeps every step's
 *       anchor variable bound before that step executes.</li>
 *   <li>Emit one {@code Element[]} per complete match, with each slot filled according to
 *       {@link GqlMatchPlan#getVariables()}, seed variable at index 0.</li>
 *   <li>Return results lazily — do not materialise the full result set in memory.</li>
 * </ul>
 */
public interface GqlExecutor {

    /**
     * Executes the plan with no parameter bindings.
     *
     * @param plan the compiled execution plan
     * @return a lazy iterator of binding arrays
     */
    Iterator<Element[]> execute(GqlMatchPlan plan);

    /**
     * Executes the plan with the given parameter bindings.
     *
     * @param plan   the compiled execution plan
     * @param params parameter bindings for {@code $name} references in property predicates;
     *               may be empty if the query contains no parameter references
     * @return a lazy iterator of binding arrays
     */
    Iterator<Element[]> execute(GqlMatchPlan plan, Map<String, Object> params);
}
