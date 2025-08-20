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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

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
public class VertexStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.out(),
                __.in(),
                __.both(),
                __.outE(),
                __.inE(),
                __.bothE(),
                __.out("knows"),
                __.out("created"),
                __.out("knows", "created"),
                __.to(Direction.OUT, "uses"),
                __.outE("knows"),
                __.inE("knows"),
                __.bothE("knows"),
                __.toE(Direction.OUT, "uses"),
                __.out(GValue.of("label", "knows")),
                __.out(GValue.of("label", "created")),
                __.out(GValue.of("label1", "knows"), GValue.of("label2", "created")),
                __.in(GValue.of("label", "knows")),
                __.both(GValue.of("label", "knows")),
                __.to(Direction.OUT, GValue.of("label", "uses")),
                __.outE(GValue.of("label", "knows")),
                __.inE(GValue.of("label", "knows")),
                __.bothE(GValue.of("label", "knows")),
                __.toE(Direction.OUT, GValue.of("label", "uses"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.out(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.out(GValue.of("label", "created")), Set.of("label")),
                Pair.of(__.out(GValue.of("label1", "knows"), GValue.of("label2", "created")), Set.of("label1", "label2")),
                Pair.of(__.in(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.both(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.to(Direction.OUT, GValue.of("label", "uses")), Set.of("label")),
                Pair.of(__.outE(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.inE(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.bothE(GValue.of("label", "knows")), Set.of("label")),
                Pair.of(__.toE(Direction.OUT, GValue.of("label", "uses")), Set.of("label"))
        );
    }

    /**
     * We used to have an issue that because we use XOR of edge labels, for example out("x") and out("x", "y", "y") are considered equal.
     * The same 2 sideEffectKey "y" are reset as a result of XOR. See: <a href="https://issues.apache.org/jira/browse/TINKERPOP-2423">TINKERPOP-2423</a>
     * This test verifies that the issue was fixed.
     */
    @Test
    public void testCheckEqualityWithRedundantEdgeLabels() {
        final Traversal<?, ?> t0 = __.both("x");
        final Traversal<?, ?> t1 = __.both("x",  "y", "y");

        assertThat(t0.asAdmin().getSteps(), hasSize(1));
        assertThat(t1.asAdmin().getSteps(), hasSize(1));
        assertThat(t0.asAdmin().getSteps().get(0), instanceOf(VertexStep.class));
        assertThat(t1.asAdmin().getSteps().get(0), instanceOf(VertexStep.class));

        final VertexStep<?> vertexStep0 = (VertexStep<?>) t0.asAdmin().getSteps().get(0);
        final VertexStep<?> vertexStep1 = (VertexStep<?>) t1.asAdmin().getSteps().get(0);
        assertThat("both(\"x\") and both(\"x\",\"y\",\"y\") must not be considered equal", vertexStep0, not(equalTo(vertexStep1)));
    }
    
    @Test
    public void getEdgeLabelsShouldPinVariables() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getVertexGValueTraversal();
        String[] edgeLabels = ((VertexStepPlaceholder<?>) traversal.getSteps().get(0)).getEdgeLabels();
        assertEquals(2, edgeLabels.length);
        assertTrue(List.of(edgeLabels).containsAll(List.of("knows", "created")));
        verifyVariables(traversal, Set.of("k", "c"), Set.of());
    }

    @Test
    public void getEdgeLabelsGValueSafeShouldNotPinVariables() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getVertexGValueTraversal();
        String[] edgeLabels = ((VertexStepPlaceholder<?>) traversal.getSteps().get(0)).getEdgeLabelsGValueSafe();
        assertEquals(2, edgeLabels.length);
        assertTrue(List.of(edgeLabels).containsAll(List.of("knows", "created")));
        verifyVariables(traversal, Set.of(), Set.of("k", "c"));
    }

    @Test
    public void getEdgeLabelsFromConcreteStep() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getVertexGValueTraversal();
        String[] edgeLabels = ((VertexStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getEdgeLabels();
        assertEquals(2, edgeLabels.length);
        assertTrue(List.of(edgeLabels).containsAll(List.of("knows", "created")));
    }
    
    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getVertexGValueTraversal();
        Collection<GValue<?>> gValues = ((VertexStepPlaceholder<?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(2, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList()).containsAll(List.of("k", "c")));
    }
    
    @Test
    public void vertexStepShouldBeUsedIfNoGValues() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = __.out("knows", "created").asAdmin();
        assertTrue(traversal.getSteps().get(0) instanceof VertexStep);
        String[] edgeLabels = ((VertexStep<?>) traversal.getSteps().get(0)).getEdgeLabels();
        assertEquals(2, edgeLabels.length);
        assertTrue(List.of(edgeLabels).containsAll(List.of("knows", "created")));
    }
    
    private GraphTraversal.Admin<Vertex, Vertex> getVertexGValueTraversal() {
        return __.out(GValue.of("k", "knows"), GValue.of("c", "created")).asAdmin();
    }
}
