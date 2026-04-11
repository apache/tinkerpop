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
package org.apache.tinkerpop.gremlin.tinkergraph.process.gql;

import java.util.Collections;
import java.util.List;

/**
 * The compiled physical execution plan for a GQL MATCH query. A {@code GqlMatchPlan} is an
 * ordered sequence of {@link ExtensionStep} objects produced by the
 * {@code TinkerGraphGqlPlanner} from a {@link QueryGraph}.
 *
 * <p>The planner selects a seed vertex (lowest-cardinality anchor) and emits one
 * {@link ExtensionStep} per edge in the {@link QueryGraph}, ordered so each step's anchor
 * variable is bound before the step executes. The executor iterates these steps to perform
 * DFS backtracking pattern matching.
 *
 * <p>The seed node is described by {@link #getSeedVariable()} and {@link #getSeedLabel()}.
 * The executor binds the seed variable first (iterating all vertices matching the seed label)
 * before processing the ordered extension steps.
 */
public final class GqlMatchPlan {

    private final String seedVariable;
    private final String seedLabel;
    private final List<ExtensionStep> steps;

    public GqlMatchPlan(final String seedVariable, final String seedLabel,
                        final List<ExtensionStep> steps) {
        this.seedVariable = seedVariable;
        this.seedLabel = seedLabel;
        this.steps = Collections.unmodifiableList(steps);
    }

    /**
     * Returns the effective variable name of the seed node. May be a synthetic name
     * (prefixed with {@code $anon}) if the seed node has no explicit variable.
     */
    public String getSeedVariable() {
        return seedVariable;
    }

    /**
     * Returns the label constraint of the seed node, or {@code null} if unconstrained.
     */
    public String getSeedLabel() {
        return seedLabel;
    }

    /**
     * Returns the ordered list of extension steps that the executor will process.
     */
    public List<ExtensionStep> getSteps() {
        return steps;
    }

    /**
     * Returns true if the plan has no steps (e.g., a single-node pattern with no edges).
     */
    public boolean isEmpty() {
        return steps.isEmpty();
    }

    @Override
    public String toString() {
        return "GqlMatchPlan{seed=" + seedVariable +
               (seedLabel != null ? ":" + seedLabel : "") +
               ", steps=" + steps + "}";
    }
}
