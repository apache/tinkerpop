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
package org.apache.tinkerpop.gremlin.structure.io.graphml;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.apache.tinkerpop.gremlin.structure.T.id;


import java.io.IOException;
import java.io.InputStream;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;

public class GraphMLReaderTest {
    final private GraphMLReader reader = GraphMLReader.build().create();

    @Test
    public void value() throws IOException {
        Graph graph = getMockedGraph();
        reader.readGraph(getSample(), graph);
        assertSingleVertex(graph,"name", "marko",
                                        "age", 29,
                                        label, "person",
                                        id, "1");
    }

    @Test
    public void defaultValue() throws IOException {
        Graph graph = getMockedGraph();
        reader.readGraph(getSample(), graph);
        assertSingleVertex(graph, "name", "alex",
                                    "lang", "gremlin",
                                    label, "person",
                                    id, "13");
    }

    /**
     * Value of a node without specified default value is empty string.
     * @throws IOException
     */
    @Test
    public void noDefaultStringValue() throws IOException {
        Graph graph = getMockedGraph();
        reader.readGraph(getSample(), graph);
        assertSingleVertex(graph, "name", "",
                label, "person",
                id, "14");
    }

    /**
     * Fails entire process if a data without default value have other format than string.
     * Reader attempts to optimistically coalesce values and don't expect empty string in most cases.
     */
    @Test
    public void noDefaultIntValue() {
        Graph graph = getMockedGraph();
        NumberFormatException e = assertThrows(NumberFormatException.class,() -> reader.readGraph(getNegativeSample(), graph));
        assertEquals("For input string: \"\"", e.getMessage());
    }

    @Test
    public void invalidDefaultValue() {
        Graph graph = getMockedGraph();
        NumberFormatException e = assertThrows(NumberFormatException.class,() -> reader.readGraph(getNegativeSampleCoercion(), graph));
        assertEquals("For input string: \"abc\"", e.getMessage());
    }

    private void assertSingleVertex(Graph graph, Object... objects) {
        verify(graph, atLeastOnce()).addVertex(objects);
    }

    private InputStream getSample() {
        return this.getClass().getClassLoader().getResourceAsStream("graphml/tinkerpop-modern.xml");
    }

    private InputStream getNegativeSample() {
        return this.getClass().getClassLoader().getResourceAsStream("graphml/tinkerpop-modern_invalid.xml");
    }

    private InputStream getNegativeSampleCoercion() {
        return this.getClass().getClassLoader().getResourceAsStream("graphml/tinkerpop-modern_invalid_coercion.xml");
    }

    private Graph getMockedGraph() {
        Graph graph = mock(Graph.class);
        Graph.Features features = mock(Graph.Features.class);
        Graph.Features.GraphFeatures graphFeatures = mock(Graph.Features.GraphFeatures.class);
        Graph.Features.VertexFeatures vertexFeatures = mock(Graph.Features.VertexFeatures.class);
        Graph.Features.EdgeFeatures edgeFeatures = mock(Graph.Features.EdgeFeatures.class);

        when(graph.addVertex(any(Object.class))).thenReturn(mock(Vertex.class));
        when(graph.features()).thenReturn(features);
        when(features.graph()).thenReturn(graphFeatures);
        when(features.vertex()).thenReturn(vertexFeatures);
        when(features.edge()).thenReturn(edgeFeatures);
        when(vertexFeatures.willAllowId(any())).thenReturn(true);
        when(graphFeatures.supportsTransactions()).thenReturn(false);
        return graph;
    }

}