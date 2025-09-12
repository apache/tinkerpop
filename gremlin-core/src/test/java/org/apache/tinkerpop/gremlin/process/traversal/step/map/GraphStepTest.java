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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class GraphStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.V(),
                __.V(1,4),
                __.V(2,3),
                __.V(GValue.of("one", 1),GValue.of("four", 4)),
                __.V(GValue.of("two", 2),3),
                __.V(1,GValue.of("four", 4))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.V(GValue.of("one", 1),GValue.of("four", 4)), Set.of("one", "four")),
                Pair.of(__.V(GValue.of("two", 2),3), Set.of("two")),
                Pair.of(__.V(1,GValue.of("four", 4)), Set.of("four"))
        );
    }

    /**
     * We used to have an issue that because we use XOR of vertex ids, V() and V("x", "x") are considered equal.
     * The same 2 vertex ids "x" are reset as a result of XOR. See: <a href="https://issues.apache.org/jira/browse/TINKERPOP-2423">TINKERPOP-2423</a>
     * This test verifies that the issue was fixed.
     */
    @Test
    public void testCheckEqualityWithRedundantIds() {
        final Traversal<?, ?> t0 = __.V();
        final Traversal<?, ?> t1 = __.V(1, 1);

        assertThat(t0.asAdmin().getSteps(), hasSize(1));
        assertThat(t1.asAdmin().getSteps(), hasSize(1));
        assertThat(t0.asAdmin().getSteps().get(0), instanceOf(GraphStep.class));
        assertThat(t1.asAdmin().getSteps().get(0), instanceOf(GraphStep.class));

        final GraphStep<?, ?> graphStep0 = (GraphStep<?, ?>) t0.asAdmin().getSteps().get(0);
        final GraphStep<?, ?> graphStep1 = (GraphStep<?, ?>) t1.asAdmin().getSteps().get(0);
        assertThat("V() and V(1,1) must not be considered equal", graphStep0, not(equalTo(graphStep1)));
    }

    @Test
    public void getIdsShouldPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getGraphStepGValueTraversal();
        assertEquals(new Object[]{1,2,3,4}, ((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0))
                .getIds());
        verifyVariables(traversal, Set.of("x","y","z"), Set.of());
    }

    @Test
    public void getIdsAsGValuesShouldNotPinVariable() {
        GraphTraversal.Admin<?, ?> traversal = getGraphStepGValueTraversal();
        assertEquals(new GValue[]{GValue.of("x", 1), GValue.of("y", 2), GValue.of(3), GValue.of("z", 4)},
                ((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0)).getIdsAsGValues());
        verifyVariables(traversal, Set.of(), Set.of("x","y","z"));
    }

    @Test
    public void getIdsFromConcreteStep() {
        GraphTraversal.Admin<?, ?> traversal = getGraphStepGValueTraversal();
        assertEquals(new Object[]{1,2,3,4}, ((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0))
                .asConcreteStep().getIds());
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<?, ?> traversal = getGraphStepGValueTraversal();
        Collection<GValue<?>> gValues = ((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(3, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("x", "y", "z")));
    }

    @Test
    public void getGValuesNonShouldReturnEmptyCollection() {
        GraphTraversal.Admin<?, ?> traversal = getGraphStepGValueTraversal();
        ((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0)).clearIds();
        assertTrue(((GraphStepPlaceholder<?,?>) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<?, ?> getGraphStepGValueTraversal() {
        return __.V(GValue.of("x", 1), GValue.of("y", 2), 3, GValue.of("z", 4))
                .asAdmin();
    }
}
