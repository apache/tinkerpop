/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.GValueManager;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.strategy.optimization.TinkerGraphStepStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TinkerGraphStepTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    /**
     * TinkerGraphStepStrategy pulls `has("age", P.gt(1))` into a HasContainer within the initial TinkerGraphStep.
     * This requires that TinkerGraphStep handles the reduction from Ternary Boolean logics (TRUE, FALSE, ERROR) to
     * ordinary boolean logics which normally takes place within FilterStep's.
     */
    @Test
    public void shouldHandleComparisonsWithNaN() {
        g.addV("v1").property("age", Double.NaN).next();
        g.addV("v1").property("age", 3).next();
        int count = g.V().has("age", P.gt(1)).count().next().intValue();
        assertEquals(1, count);
    }

    @Test
    public void getPredicatesShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getTinkerGraphStepGValueTraversal();
        assertEquals(List.of("marko", 25), ((TinkerGraphStep<?,?>) traversal.getSteps().get(0))
                .getPredicates().stream().map(P::getValue).collect(Collectors.toList()));
        verifyVariables(traversal, Set.of("n", "a"), Set.of());
    }

    @Test
    public void getPredicatesGValueSafeShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getTinkerGraphStepGValueTraversal();
        assertEquals(List.of("marko", 25), ((TinkerGraphStep<?,?>) traversal.getSteps().get(0))
                .getPredicatesGValueSafe().stream().map(P::getValue).collect(Collectors.toList()));
        verifyVariables(traversal, Set.of(), Set.of("n", "a"));
    }

    @Test
    public void getPredicatesFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getTinkerGraphStepGValueTraversal();
        assertEquals(List.of("marko", 25), ((TinkerGraphStep<?,?>) traversal.getSteps().get(0))
                .asConcreteStep().getPredicates().stream().map(P::getValue).collect(Collectors.toList()));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<?, ?> traversal = getTinkerGraphStepGValueTraversal();
        Collection<GValue<?>> gValues = ((TinkerGraphStep<?,?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(2, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("n", "a")));
    }

    @Test
    public void getGValuesNonShouldReturnEmptyCollection() {
        GraphTraversal.Admin<?, ?> traversal = TinkerGraph.open().traversal()
                .V()
                .has("name", "marko")
                .has("age", P.gt(25))
                .asAdmin();
        TinkerGraphStepStrategy.instance().apply(traversal);
        assertTrue(((HasContainerHolder<?,?>) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<?, ?> getTinkerGraphStepGValueTraversal() {
        GraphTraversal.Admin<?, ?> traversal = TinkerGraph.open().traversal()
                .V()
                .has("name", GValue.of("n", "marko"))
                .has("age", P.gt(GValue.of("a", 25)))
                .asAdmin();
        TinkerGraphStepStrategy.instance().apply(traversal);
        return traversal;
    }

    private void verifyVariables(GraphTraversal.Admin<?, ?> traversal, Set<String> pinnedVariables, Set<String> unpinnedVariables) {
        GValueManager gValueManager = traversal.getGValueManager();
        assertEquals(pinnedVariables, gValueManager.getPinnedVariableNames());
        assertEquals(unpinnedVariables, gValueManager.getUnpinnedVariableNames());
        if (!unpinnedVariables.isEmpty()) {
            assertTrue(gValueManager.hasUnpinnedVariables());
        } else {
            assertFalse(gValueManager.hasUnpinnedVariables());
        }
    }
}
