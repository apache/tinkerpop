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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.constant;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Tests for addLabel() and dropLabel() traversal steps, addV multi-label,
 * and valueMap/elementMap multilabel configuration.
 */
public class LabelMutationStepTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        graph = TinkerGraph.open();
        g = graph.traversal();
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    // --- addLabel step tests ---

    @Test
    public void shouldAddLabelViaTraversal() {
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel("employee").iterate();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldAddLabelWithConstantTraversal() {
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel(constant("manager")).iterate();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "manager"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowWhenAddingLabelToEdgeViaTraversal() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        v1.addEdge("knows", v2);
        g.E().addLabel("friend").iterate();
    }

    // --- dropLabel step tests ---

    @Test
    public void shouldDropSpecificLabelViaTraversal() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        g.V(v).dropLabel("person").iterate();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("employee"));
    }

    @Test
    public void shouldDropAllLabelsViaTraversal() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        g.V(v).dropLabels().iterate();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
    }

    @Test
    public void shouldDropLabelWithConstantTraversal() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        g.V(v).dropLabel(constant("person")).iterate();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("employee"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldThrowWhenDroppingLabelsOnEdgeViaTraversal() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        v1.addEdge("knows", v2);
        g.E().dropLabels().iterate();
    }

    // --- addV multi-label tests ---

    @Test
    public void shouldCreateVertexWithMultipleLabelsViaAddV() {
        final Vertex v = g.addV("a").addLabel("b").next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("a", "b"));
    }

    @Test
    public void shouldCreateVertexWithDefaultLabelViaAddV() {
        final Vertex v = g.addV().next();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
    }

    // --- hasLabel index consistency tests ---

    @Test
    public void shouldFindVertexByLabelAfterAddLabel() {
        final Vertex v = g.addV("person").next();
        g.V(v).addLabel("employee").iterate();
        final List<Vertex> found = g.V().hasLabel("employee").toList();
        assertThat(found, hasSize(1));
        assertThat(found.get(0).id(), is(v.id()));
    }

    @Test
    public void shouldNotFindVertexByLabelAfterDropLabel() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        g.V(v).dropLabel("employee").iterate();
        final List<Vertex> found = g.V().hasLabel("employee").toList();
        assertThat(found, hasSize(0));
    }

    @Test
    public void shouldFindVertexByBothLabelsWithChainedHasLabel() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final List<Vertex> found = g.V().hasLabel("person").hasLabel("employee").toList();
        assertThat(found, hasSize(1));
        assertThat(found.get(0).id(), is(v.id()));
    }

    @Test
    public void shouldNotFindVertexMissingOneOfChainedHasLabels() {
        final Vertex v = g.addV("person").next();
        final List<Vertex> found = g.V().hasLabel("person").hasLabel("employee").toList();
        assertThat(found, hasSize(0));
    }

    // --- valueMap/elementMap multilabel config tests ---

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnLabelsAsSetWithMultilabelConfig() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final Map<Object, Object> map = g.V(v).valueMap(true)
                .with(WithOptions.multilabel).next();
        final Object labelValue = map.get(T.label);
        assertThat(labelValue, instanceOf(Set.class));
        final Set<String> labels = (Set<String>) labelValue;
        assertThat(labels, containsInAnyOrder("person", "employee"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReturnLabelAsSingleStringWithoutMultilabelConfig() {
        final Vertex v = g.addV("person").next();
        final Map<Object, Object> map = g.V(v).valueMap(true).next();
        final Object labelValue = map.get(T.label);
        assertThat(labelValue, instanceOf(String.class));
        assertThat((String) labelValue, is("person"));
    }

    // --- addLabel pass-through (chaining) test ---

    @Test
    public void shouldReturnSameVertexAfterAddLabel() {
        final Vertex v = g.addV("person").next();
        final Vertex result = g.V(v).addLabel("employee").next();
        assertThat(result.id(), is(v.id()));
        assertThat(result.labels(), containsInAnyOrder("person", "employee"));
    }

    // --- dropLabel pass-through (chaining) test ---

    @Test
    public void shouldReturnSameVertexAfterDropLabel() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final Vertex result = g.V(v).dropLabel("employee").next();
        assertThat(result.id(), is(v.id()));
    }

    // --- elementMap multilabel config test ---

    @SuppressWarnings("unchecked")
    @Test
    public void shouldReturnLabelsAsSetWithElementMapMultilabelConfig() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final Map<Object, Object> map = g.V(v).elementMap()
                .with(WithOptions.multilabel).next();
        final Object labelValue = map.get(T.label);
        assertThat(labelValue, instanceOf(Set.class));
        final Set<String> labels = (Set<String>) labelValue;
        assertThat(labels, containsInAnyOrder("person", "employee"));
    }

    // --- GraphTraversalSource multi-label addV test ---

    @Test
    public void shouldCreateVertexWithMultipleLabelsFromSource() {
        final Vertex v = g.addV("person", "employee").next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldCreateVertexWithThreeLabelsFromSource() {
        final Vertex v = g.addV("person", "employee", "manager").next();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("person", "employee", "manager"));
    }
}
