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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for mergeV() multi-label support.
 */
public class MergeVMultiLabelTest {

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

    // --- mergeV create with multi-label ---

    @Test
    public void shouldCreateVertexWithMultiLabelViaMergeV() {
        final Set<String> labels = new LinkedHashSet<>(Arrays.asList("person", "employee"));
        final Vertex v = g.mergeV(Map.of(T.label, labels, "name", "marko")).next();
        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("person", "employee"));
        assertThat(v.value("name"), is("marko"));
    }

    @Test
    public void shouldCreateVertexWithSingleLabelViaMergeV() {
        final Vertex v = g.mergeV(Map.of(T.label, "person", "name", "marko")).next();
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("person"));
    }

    @Test
    public void shouldCreateVertexWithListLabelViaMergeV() {
        final List<String> labels = Arrays.asList("a", "b", "c");
        final Vertex v = g.mergeV(Map.of(T.label, labels, "name", "test")).next();
        assertThat(v.labels(), hasSize(3));
        assertThat(v.labels(), containsInAnyOrder("a", "b", "c"));
    }

    // --- mergeV match with multi-label ---

    @Test
    public void shouldMatchVertexWithMultiLabelViaMergeV() {
        // create a vertex with two labels
        final Vertex existing = g.addV("person").addLabel("employee").property("name", "marko").next();

        // mergeV should match it using AND semantics
        final Set<String> labels = new LinkedHashSet<>(Arrays.asList("person", "employee"));
        final Vertex matched = g.mergeV(Map.of(T.label, labels, "name", "marko")).next();

        assertThat(matched.id(), is(existing.id()));
        // should not create a new vertex
        assertThat(g.V().count().next(), is(1L));
    }

    @Test
    public void shouldNotMatchWhenVertexMissingOneLabel() {
        // create a vertex with only one label
        g.addV("person").property("name", "marko").next();

        // mergeV with two labels should NOT match (AND semantics)
        final Set<String> labels = new LinkedHashSet<>(Arrays.asList("person", "employee"));
        g.mergeV(Map.of(T.label, labels, "name", "marko")).next();

        // should have created a new vertex
        assertThat(g.V().count().next(), is(2L));
    }

    // --- mergeV onMatch label replacement ---

    @Test
    public void shouldReplaceLabelsOnMatchWithSingleLabel() {
        final Vertex v = g.addV("person").addLabel("employee").property("name", "marko").next();

        g.mergeV(Map.of(T.label, "person", "name", "marko"))
                .option(Merge.onMatch, Map.of(T.label, "manager")).next();

        // labels should be wholly replaced
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder("manager"));
    }

    @Test
    public void shouldReplaceLabelsOnMatchWithMultiLabel() {
        final Vertex v = g.addV("person").property("name", "marko").next();

        final Set<String> newLabels = new LinkedHashSet<>(Arrays.asList("manager", "director"));
        g.mergeV(Map.of(T.label, "person", "name", "marko"))
                .option(Merge.onMatch, Map.of(T.label, newLabels)).next();

        assertThat(v.labels(), hasSize(2));
        assertThat(v.labels(), containsInAnyOrder("manager", "director"));
    }

    @Test
    public void shouldApplyDefaultLabelOnMatchWithEmptyCollection() {
        final Vertex v = g.addV("person").property("name", "marko").next();

        g.mergeV(Map.of(T.label, "person", "name", "marko"))
                .option(Merge.onMatch, Map.of(T.label, Collections.emptySet())).next();

        // empty collection triggers default label behavior
        assertThat(v.labels(), hasSize(1));
        assertThat(v.labels(), containsInAnyOrder(Vertex.DEFAULT_LABEL));
    }
}
