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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
 * <p>The seed node is described by {@link #getSeedVariable()}, {@link #getSeedLabel()}, and
 * {@link #getSeedPredicates()}. The executor binds the seed variable first (iterating all
 * vertices matching the seed label and property predicates) before processing the extension steps.
 */
public final class GqlMatchPlan {

    /**
     * Prefix used for synthetic variable names assigned to anonymous pattern nodes/edges
     * (those written without an explicit variable, e.g. {@code ()-[:knows]->()} ).
     * Variables with this prefix are internal bookkeeping and are never exposed in result
     * binding maps or traverser paths.
     */
    public static final String ANON_VAR_PREFIX = "$anon";

    private final String seedVariable;
    private final String seedLabel;
    private final List<PropertyPredicate> seedPredicates;
    private final List<ExtensionStep> steps;
    /**
     * Maps every variable name (including {@code $anon*} synthetics) to its slot index in
     * the {@code Element[]} binding array used by the executor. Seed variable is always at
     * index 0; subsequent indices follow step order (edge variable before target variable
     * for each step).
     */
    private final Map<String, Integer> variableIndex;
    /**
     * Inverse of {@code variableIndex}: index → variable name. Used by
     * {@code TinkerGraphMatchStep} to iterate result rows without an extra map lookup.
     */
    private final String[] variables;

    public GqlMatchPlan(final String seedVariable, final String seedLabel,
                        final List<ExtensionStep> steps,
                        final Map<String, Integer> variableIndex,
                        final String[] variables,
                        final List<PropertyPredicate> seedPredicates) {
        this.seedVariable = seedVariable;
        this.seedLabel = seedLabel;
        this.seedPredicates = seedPredicates.isEmpty()
                ? Collections.emptyList()
                : Collections.unmodifiableList(new ArrayList<>(seedPredicates));
        this.steps = Collections.unmodifiableList(steps);
        this.variableIndex = variableIndex;
        this.variables = variables;
    }

    public GqlMatchPlan(final String seedVariable, final String seedLabel,
                        final List<ExtensionStep> steps,
                        final Map<String, Integer> variableIndex,
                        final String[] variables) {
        this(seedVariable, seedLabel, steps, variableIndex, variables, Collections.emptyList());
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
     * Returns property equality predicates on the seed node derived from its inline filter
     * map. The executor applies these when iterating seed candidates. Empty if no filter
     * was specified on the seed node.
     */
    public List<PropertyPredicate> getSeedPredicates() {
        return seedPredicates;
    }

    /**
     * Returns the ordered list of extension steps that the executor will process.
     */
    public List<ExtensionStep> getSteps() {
        return steps;
    }

    /** Returns the index of the seed variable in the binding array. Always 0. */
    public int getSeedVariableIndex() {
        return 0;
    }

    /** Returns the binding-array index for the given variable name. */
    public int getIndex(final String variableName) {
        final Integer idx = variableIndex.get(variableName);
        if (idx == null)
            throw new IllegalArgumentException("Unknown variable in GQL plan: '" + variableName + "'");
        return idx;
    }

    /** Returns the total number of variables (= binding array length). */
    public int getVariableCount() {
        return variables.length;
    }

    /**
     * Returns the variable name at each binding-array index. Used by
     * {@code TinkerGraphMatchStep} to iterate result rows by index.
     */
    public String[] getVariables() {
        return variables;
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
               (seedPredicates.isEmpty() ? "" : " " + seedPredicates) +
               ", steps=" + steps + "}";
    }
}
