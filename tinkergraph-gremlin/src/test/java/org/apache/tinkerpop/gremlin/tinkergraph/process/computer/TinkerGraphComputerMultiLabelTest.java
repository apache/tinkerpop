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
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

/**
 * Verifies that a {@code ResultGraph.NEW} produced by {@link org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer}
 * preserves multi-label and zero-label vertices rather than collapsing them to a single label (which previously
 * threw {@code "Label can not be empty"} for zero-label vertices).
 */
public class TinkerGraphComputerMultiLabelTest {

    private static TinkerGraph openMultiLabelGraph() {
        final Configuration config = new BaseConfiguration();
        config.setProperty(AbstractTinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_LABEL_CARDINALITY, "ZERO_OR_MORE");
        return TinkerGraph.open(config);
    }

    private static Graph computeNewGraph(final TinkerGraph graph, final GraphComputer.Persist persist) throws Exception {
        final ComputerResult result = graph.compute()
                .program(ConnectedComponentVertexProgram.build().create(graph))
                .result(GraphComputer.ResultGraph.NEW)
                .persist(persist)
                .submit().get();
        return result.graph();
    }

    @Test
    public void shouldPreserveVertexLabelsInNewResultGraphWithPersistEdges() throws Exception {
        try (final TinkerGraph graph = openMultiLabelGraph()) {
            final Vertex a = graph.addVertex(T.id, "a", T.label, Arrays.asList("person", "employee"));
            final Vertex b = graph.addVertex(T.id, "b", T.label, "person");
            graph.addVertex(T.id, "c", T.label, Collections.emptyList());
            a.addEdge("knows", b);

            final Graph result = computeNewGraph(graph, GraphComputer.Persist.EDGES);

            assertThat(result.vertices("a").next().labels(), containsInAnyOrder("person", "employee"));
            assertThat(result.vertices("b").next().labels(), containsInAnyOrder("person"));
            assertThat(result.vertices("c").next().labels(), hasSize(0));
            assertEquals(1L, result.traversal().E().count().next().longValue());
        }
    }

    @Test
    public void shouldPreserveVertexLabelsInNewResultGraphWithPersistVertexProperties() throws Exception {
        try (final TinkerGraph graph = openMultiLabelGraph()) {
            final Vertex a = graph.addVertex(T.id, "a", T.label, Arrays.asList("person", "employee"));
            final Vertex b = graph.addVertex(T.id, "b", T.label, "person");
            graph.addVertex(T.id, "c", T.label, Collections.emptyList());
            a.addEdge("knows", b);

            final Graph result = computeNewGraph(graph, GraphComputer.Persist.VERTEX_PROPERTIES);

            assertThat(result.vertices("a").next().labels(), containsInAnyOrder("person", "employee"));
            assertThat(result.vertices("b").next().labels(), containsInAnyOrder("person"));
            assertThat(result.vertices("c").next().labels(), hasSize(0));
            assertEquals(0L, result.traversal().E().count().next().longValue());
        }
    }
}
