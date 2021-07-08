package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.thrift.TException;
import org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain.MyApplication;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.IOException;

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

        Graph graph;

        try {
            graph = TinkerGraph.open(conf);
        } catch (Exception e) {
            throw new AddGraphException("could not open graph: " + e.getMessage());
        }

        encoding.toNativeGraph(graph, g0);

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
