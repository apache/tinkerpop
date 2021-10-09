package org.apache.tinkerpop.gremlin.structure.io.graphml;

import junit.framework.TestCase;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import static org.apache.tinkerpop.gremlin.structure.T.label;
import static org.apache.tinkerpop.gremlin.structure.T.id;


import java.io.IOException;
import java.io.InputStream;


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;

public class GraphMLReaderTest extends TestCase {
    final private GraphMLReader reader = GraphMLReader.build().create();

    public void testValue() throws IOException {
        Graph graph = getMockedGraph();
        reader.readGraph(getSample(), graph);
        assertSingleVertex(graph,"modification", "delete",
                                        label, "vertex",
                                        id, "10380");
    }

    public void testDefaultValue() throws IOException {
        Graph graph = getMockedGraph();
        reader.readGraph(getSample(), graph);
        assertSingleVertex(graph, "modification", "add",
                                    label, "vertex",
                                    id, "6315");
    }

    private void assertSingleVertex(Graph graph, Object... objects) {
        verify(graph, atLeastOnce()).addVertex(objects);
    }

    private InputStream getSample() {
        return this.getClass().getClassLoader().getResourceAsStream("graphml/sample.graphml.xml");
    }

    private Graph getMockedGraph() {
        Graph graph = mock(Graph.class);
        Graph.Features features = mock(Graph.Features.class);
        Graph.Features.GraphFeatures graphFeatures = mock(Graph.Features.GraphFeatures.class);
        Graph.Features.VertexFeatures vertexFeatures = mock(Graph.Features.VertexFeatures.class);

        when(graph.addVertex(any(Object.class))).thenReturn(mock(Vertex.class));
        when(graph.features()).thenReturn(features);
        when(features.graph()).thenReturn(graphFeatures);
        when(features.vertex()).thenReturn(vertexFeatures);
        when(vertexFeatures.willAllowId(any())).thenReturn(true);
        when(graphFeatures.supportsTransactions()).thenReturn(false);
        return graph;
    }

}