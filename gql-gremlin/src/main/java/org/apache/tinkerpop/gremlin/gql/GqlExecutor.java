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

import java.util.Iterator;
import java.util.Map;

/**
 * Executes a {@link GqlMatchPlan} against a graph and returns a lazy iterator of {@link GqlRow}
 * result rows. {@link DefaultGqlExecutor} is the reference implementation.
 *
 * <h3>When to implement this interface</h3>
 *
 * <p>This is the primary customization point for providers with a native query or traversal
 * engine. Such a provider can keep {@link DefaultGqlPlanner}, getting GQL MATCH parsing and
 * cardinality-guided join ordering for free, and replace only the executor to translate the
 * {@link GqlMatchPlan} into native operations.</p>
 *
 * <p>The contract for implementors:</p>
 * <ul>
 *   <li>Iterate seed vertex candidates matching the plan's seed label and predicates.</li>
 *   <li>For each seed, extend the partial match through the plan's
 *       {@link GqlMatchPlan#getSteps() extension steps} in an order that keeps every step's
 *       anchor variable bound before that step executes.</li>
 *   <li>Emit one {@link GqlRow} per complete match, populated as described on {@link GqlRow}.</li>
 *   <li>Return results lazily rather than materialising the full result set in memory.</li>
 * </ul>
 */
public interface GqlExecutor {

    /**
     * Executes the plan with no parameter bindings.
     *
     * @param plan the compiled execution plan
     * @return a lazy iterator of result rows
     */
    Iterator<GqlRow> execute(GqlMatchPlan plan);

    /**
     * Executes the plan with the given parameter bindings.
     *
     * @param plan   the compiled execution plan
     * @param params parameter bindings for {@code $name} references in property predicates;
     *               may be empty if the query contains no parameter references
     * @return a lazy iterator of result rows
     */
    Iterator<GqlRow> execute(GqlMatchPlan plan, Map<String, Object> params);
}
