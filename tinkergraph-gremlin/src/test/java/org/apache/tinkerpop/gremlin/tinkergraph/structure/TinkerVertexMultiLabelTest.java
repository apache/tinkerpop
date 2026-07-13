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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for multi-label support on TinkerVertex.
 */
public class TinkerVertexMultiLabelTest {

    private Graph graph;
    private GraphTraversalSource g;

    @Before
    public void setup() {
        final org.apache.commons.configuration2.Configuration config = new org.apache.commons.configuration2.BaseConfiguration();
        config.setProperty(Graph.GRAPH, TinkerGraph.class.getName());
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ZERO_OR_MORE");
        graph = TinkerGraph.open(config);
        g = graph.traversal();
    }

    @After
    public void tearDown() throws Exception {
        graph.close();
    }

    @Test
    public void shouldCreateVertexWithSingleLabel() {
        final Vertex v = g.addV("person").next();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test
    public void shouldCreateVertexWithMultipleLabels() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldCreateVertexWithDefaultLabelWhenNoneSpecified() {
        final Vertex v = g.addV().next();
        // Under ZERO_OR_MORE cardinality, addV() with no label produces an empty label set
        assertThat(v.labels(), hasSize(0));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldReturnFirstLabelFromDeprecatedLabelMethod() {
        final Vertex v = g.addV("person").next();
        assertThat(v.label(), is("person"));
        assertThat(v.labels().contains(v.label()), is(true));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldReturnUnmodifiableSetFromLabels() {
        final Vertex v = g.addV("person").next();
        v.labels().add("hacker");
    }

    @Test
    public void shouldAddLabelToExistingVertex() {
        final Vertex v = g.addV("person").next();
        v.addLabel("employee");
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldBeIdempotentWhenAddingExistingLabel() {
        final Vertex v = g.addV("person").next();
        v.addLabel("person");
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenAddingNullLabel() {
        final Vertex v = g.addV("person").next();
        v.addLabel(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenAddingEmptyLabel() {
        final Vertex v = g.addV("person").next();
        v.addLabel("");
    }

    @Test
    public void shouldDropAllLabelsAndAssignDefault() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        v.dropLabels();
        // Under ZERO_OR_MORE cardinality, dropping all labels results in empty set (no virtual default)
        assertThat(v.labels(), hasSize(0));
    }

    @Test
    public void shouldDropSpecificLabel() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        v.dropLabel("person");
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("employee"));
    }

    @Test
    public void shouldBeNoOpWhenDroppingNonExistentLabel() {
        final Vertex v = g.addV("person").next();
        final Set<String> before = Set.copyOf(v.labels());
        v.dropLabel("nonexistent");
        assertThat(v.labels(), is(before));
    }

    @Test
    public void shouldAssignDefaultWhenDroppingLastSpecificLabel() {
        final Vertex v = g.addV("person").next();
        v.dropLabel("person");
        // Under ZERO_OR_MORE cardinality, dropping last label results in empty set (no virtual default)
        assertThat(v.labels(), hasSize(0));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldAddLabelToEdge() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        final Edge e = v1.addEdge("knows", v2);
        // Under ZERO_OR_ONE cardinality for edges, adding a second label throws
        e.addLabel("friend");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldDropLabelsOnEdge() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        final Edge e = v1.addEdge("knows", v2);
        // Edge cardinality is always ONE  - dropLabels() throws
        e.dropLabels();
    }

    @Test
    public void shouldReturnSingletonSetForEdgeLabels() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        final Edge e = v1.addEdge("knows", v2);
        assertThat(e.labels(), hasSize(1));
        assertThat(e.labels(), containsInAnyOrder("knows"));
    }

    @Test
    public void shouldAddLabelToExistingVertexWithDefaultLabel() {
        // Under ZERO_OR_MORE, addV() assigns no labels. Adding "person" creates a single-label set.
        final Vertex v = g.addV().next();
        assertThat(v.labels(), hasSize(0));
        v.addLabel("person");
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test
    public void shouldDeduplicateLabelsOnAddV() {
        final Vertex v = g.addV("person").addLabel("person").next();
        // addLabel("person") on a vertex already labeled "person" is idempotent
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test
    public void shouldUpdateVertexLabelCountWhenAddingLabel() {
        final Vertex v = g.addV("person").next();
        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(0L));

        v.addLabel("employee");

        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(1L));
    }

    @Test
    public void shouldUpdateVertexLabelCountWhenDroppingSpecificLabel() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(1L));

        v.dropLabel("person");

        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(0L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(1L));
    }

    @Test
    public void shouldUpdateVertexLabelCountWhenDroppingAllLabels() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(1L));

        v.dropLabels();

        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(0L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(0L));
    }

    @Test
    public void shouldNotChangeVertexLabelCountWhenAddingIdempotentLabel() {
        final Vertex v = g.addV("person").next();
        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));

        v.addLabel("person");

        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(1L));
    }

    @Test
    public void shouldCountVerticesWithSharedLabelRegardlessOfOtherLabels() {
        g.addV("person").next();
        g.addV("person").addLabel("employee").next();
        g.addV("person").addLabel("customer").next();

        assertThat(((TinkerGraph) graph).countVerticesByLabel("person"), is(3L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("employee"), is(1L));
        assertThat(((TinkerGraph) graph).countVerticesByLabel("customer"), is(1L));
    }
}
