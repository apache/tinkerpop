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

/**
 * Compiles a GQL MATCH string into an executable {@link GqlMatchPlan}.
 *
 * <p>{@link DefaultGqlPlanner} is the reference implementation. It parses the GQL MATCH
 * expression into a {@link QueryGraph}, selects a seed vertex by lowest cardinality (via
 * {@link org.apache.tinkerpop.gremlin.structure.Graph#countVerticesByLabel(String)}), and
 * orders extension steps by edge-label density (via
 * {@link org.apache.tinkerpop.gremlin.structure.Graph#countEdgesByLabel(String)}) through a
 * BFS traversal of the query graph. The parsed {@link QueryGraph} is cached by query string;
 * seed selection and step ordering are recomputed on each call from live counts so that graph
 * mutations are always reflected.</p>
 *
 * <h3>When to implement this interface</h3>
 *
 * <p>Most providers do not need to implement {@code GqlPlanner}. The statistics that drive
 * planning — label cardinalities and index counts — are already surfaced as default methods on
 * {@link org.apache.tinkerpop.gremlin.structure.Graph} ({@code countVerticesByLabel},
 * {@code countEdgesByLabel}, and {@link org.apache.tinkerpop.gremlin.structure.Graph.Index});
 * overriding those methods is all that is needed to improve plan quality in most cases, and
 * {@link DefaultGqlPlanner} consults them automatically.</p>
 *
 * <p>A custom {@code GqlPlanner} is warranted only when a provider needs a fundamentally
 * different join strategy — for example, a cost-based optimizer backed by richer statistics
 * than label counts. Note that producing a valid {@link GqlMatchPlan} requires constructing
 * {@link ExtensionStep} objects, building a variable index map, and satisfying all other
 * invariants of that concrete class — non-trivial work that is roughly comparable in effort
 * to writing a custom executor. At that level of investment, consider whether replacing both
 * planner and executor, or bypassing {@code gql-gremlin} entirely by implementing a
 * {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.ProviderOptimizationStrategy}
 * that claims
 * {@link org.apache.tinkerpop.gremlin.process.traversal.step.map.DeclarativeMatchStep} directly,
 * is the cleaner approach.</p>
 *
 * <p>The more common advanced customization is to keep {@link DefaultGqlPlanner} and replace
 * only the executor. See {@link GqlExecutor} for details.</p>
 *
 * <p>Implementations are encouraged to cache the parsed {@link QueryGraph} keyed on the query
 * string; seed selection should be recomputed on each call so that graph mutations are reflected
 * in subsequent executions.</p>
 */
public interface GqlPlanner {

    /**
     * Compiles the given GQL MATCH expression into an executable plan.
     *
     * @param gqlMatchString a GQL MATCH expression (e.g. {@code "MATCH (a:Person)-[:KNOWS]->(b)"})
     * @return the compiled execution plan
     * @throws IllegalArgumentException if the string cannot be parsed
     */
    GqlMatchPlan plan(String gqlMatchString);
}
