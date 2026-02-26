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

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the labels() traversal step.
 */
public class LabelsStepTest {

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

    @Test
    public void shouldStreamAllLabelsFromVertex() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final List<String> labels = g.V(v).labels().toList();
        assertThat(labels, hasSize(2));
        assertThat(labels, containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldCountLabelsCorrectly() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final long count = g.V(v).labels().count().next();
        assertThat(count, is(2L));
    }

    @Test
    public void shouldFoldAllLabels() {
        final Vertex v = g.addV("person").addLabel("employee").next();
        final List<String> folded = g.V(v).labels().fold().next();
        assertThat(folded, hasSize(2));
        assertThat(folded, containsInAnyOrder("person", "employee"));
    }

    @Test
    public void shouldReturnSingletonForEdge() {
        final Vertex v1 = g.addV("person").next();
        final Vertex v2 = g.addV("person").next();
        final Edge e = v1.addEdge("knows", v2);
        final List<String> labels = g.E(e).labels().toList();
        assertThat(labels, hasSize(1));
        assertThat(labels, containsInAnyOrder("knows"));
    }

    @Test
    public void shouldStreamSingleLabelFromSingleLabelVertex() {
        final Vertex v = g.addV("person").next();
        final List<String> labels = g.V(v).labels().toList();
        assertThat(labels, hasSize(1));
        assertThat(labels, containsInAnyOrder("person"));
    }

    @Test
    public void shouldStreamDefaultLabelFromDefaultVertex() {
        final Vertex v = g.addV().next();
        final List<String> labels = g.V(v).labels().toList();
        assertThat(labels, hasSize(1));
        assertThat(labels, containsInAnyOrder(Vertex.DEFAULT_LABEL));
    }
}
