package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.thrift.TException;
import org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain.MyApplication;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AddGraphHandler implements ExampleService.Iface {
    private final GraphEncoding encoding = new GraphEncoding(MyApplication.SUPPORTED_ENCODINGS);

    @Override
    public AddGraphResponse addGraph(org.apache.tinkerpop.gremlin.language.property_graphs.Graph g0)
            throws AddGraphException, TException {
        try {
            return addGraphInternal(g0);
        } catch (IOException e) {
            e.printStackTrace();
            throw new AddGraphException("IO exception: " + e.getMessage());
        }
    }

    private AddGraphResponse addGraphInternal(org.apache.tinkerpop.gremlin.language.property_graphs.Graph g0)
            throws AddGraphException, IOException {
        Configuration conf = new BaseConfiguration();
        conf.setProperty("gremlin.tinkergraph.graphLocation", "/tmp/graphs-over-thrift.json");
        conf.setProperty("gremlin.tinkergraph.graphFormat", "graphson");
        //conf.setProperty("gremlin.tinkergraph.graphLocation", "/tmp/graphs-over-thrift.xml");
        //conf.setProperty("gremlin.tinkergraph.graphFormat", "graphml");
        Graph graph;

        try {
            graph = TinkerGraph.open(conf);
        } catch (Exception e) {
            throw new AddGraphException("could not open graph: " + e.getMessage());
        }

        // Add vertices
        Map<Object, Vertex> vertices = new HashMap<>();
        for (org.apache.tinkerpop.gremlin.language.property_graphs.Vertex v0 : g0.vertices) {
            Object id = encoding.toNativeAtomicValue(v0.id);
            Vertex v = graph.addVertex(T.id, id);
            for (org.apache.tinkerpop.gremlin.language.property_graphs.Property p0 : v0.properties) {
                v.property(p0.key, encoding.toNativeValue(p0.value));
            }

            vertices.put(id, v);
        }

        // Add edges
        for (org.apache.tinkerpop.gremlin.language.property_graphs.Edge e0 : g0.edges) {
            Object id = encoding.toNativeAtomicValue(e0.id);
            Object outId = encoding.toNativeAtomicValue(e0.outVertexId);
            Object inId = encoding.toNativeAtomicValue(e0.inVertexId);

            Vertex out = vertices.get(outId);
            Vertex in = vertices.get(inId);
            if (out == null || in == null) {
                throw new AddGraphException("edge references vertices which are not in the graph");
            }

            Edge e = out.addEdge(e0.label, in, T.id, id);

            for (org.apache.tinkerpop.gremlin.language.property_graphs.Property p0 : e0.properties) {
                e.property(p0.key, encoding.toNativeValue(p0.value));
                //System.out.println("Thrift value: " + p0.value + ", native value: " + GraphThriftUtils.toNativeValue(p0.value));
            }
        }

        GraphTraversalSource g = graph.traversal();
        long vertexCount = g.V().count().next();
        long edgeCount = g.E().count().next();

        for (Property<Object> p : g.V().properties("livesIn").toList()) {
            System.out.println("Found a livesIn value: " + p.value());
        }

        try {
            // This should cause the graph to be saved to the configured location
            graph.close();
        } catch (Exception e) {
            throw new AddGraphException("failed to close graph: " + e.getMessage());
        }

        AddGraphResponse response = new AddGraphResponse();
        response.setTotalVerticesAfterOperation(vertexCount);
        response.setTotalEdgesAfterOperation(edgeCount);

        return response;
    }
}
