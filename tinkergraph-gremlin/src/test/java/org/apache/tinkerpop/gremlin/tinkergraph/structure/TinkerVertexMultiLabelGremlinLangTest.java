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

import org.apache.tinkerpop.gremlin.process.remote.EmbeddedRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.containsString;

/**
 * Tests that verify GremlinLang output for multi-label addV does not double-record
 * addLabel steps, and that the EmbeddedRemoteConnection path works correctly.
 */
public class TinkerVertexMultiLabelGremlinLangTest {

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
    public void shouldNotDoubleRecordAddLabelInGremlinLangFromSource() {
        final GraphTraversal<Vertex, Vertex> t = g.addV("a", "b");
        final String gremlinLang = t.asAdmin().getGremlinLang().getGremlin();
        // Should contain addV("a","b") but NOT a trailing addLabel
        assertThat(gremlinLang, not(containsString("addLabel")));
    }

    @Test
    public void shouldNotDoubleRecordAddLabelInGremlinLangFromTraversal() {
        final GraphTraversal<Vertex, Vertex> t = g.V().addV("a", "b");
        final String gremlinLang = t.asAdmin().getGremlinLang().getGremlin();
        // Should contain addV("a","b") but NOT a trailing addLabel
        assertThat(gremlinLang, not(containsString("addLabel")));
    }

    @Test
    public void shouldNotDoubleRecordAddLabelWithThreeLabelsFromSource() {
        final GraphTraversal<Vertex, Vertex> t = g.addV("a", "b", "c");
        final String gremlinLang = t.asAdmin().getGremlinLang().getGremlin();
        assertThat(gremlinLang, not(containsString("addLabel")));
    }

    @Test
    public void shouldCreateExactlyOneVertexViaEmbeddedRemoteConnection() throws Exception {
        final GraphTraversalSource remoteG = traversal().with(new EmbeddedRemoteConnection(g));
        try {
            final List<Vertex> vertices = remoteG.addV("a", "b").toList();
            assertThat(vertices, hasSize(1));
            assertThat(vertices.get(0).labels(), hasSize(2));
            assertThat(vertices.get(0).labels(), containsInAnyOrder("a", "b"));
        } finally {
            remoteG.close();
        }
    }

    @Test
    public void shouldCreateExactlyOneVertexWithThreeLabelsViaEmbeddedRemote() throws Exception {
        final GraphTraversalSource remoteG = traversal().with(new EmbeddedRemoteConnection(g));
        try {
            final List<Vertex> vertices = remoteG.addV("x", "y", "z").toList();
            assertThat(vertices, hasSize(1));
            assertThat(vertices.get(0).labels(), hasSize(3));
            assertThat(vertices.get(0).labels(), containsInAnyOrder("x", "y", "z"));
        } finally {
            remoteG.close();
        }
    }

    @Test
    public void shouldNotDuplicateVerticesOnRepeatedEmbeddedRemoteCalls() throws Exception {
        final GraphTraversalSource remoteG = traversal().with(new EmbeddedRemoteConnection(g));
        try {
            remoteG.addV("a", "b").iterate();
            remoteG.addV("a", "b").iterate();
            final long count = g.V().count().next();
            assertThat(count, is(2L));
        } finally {
            remoteG.close();
        }
    }
}
