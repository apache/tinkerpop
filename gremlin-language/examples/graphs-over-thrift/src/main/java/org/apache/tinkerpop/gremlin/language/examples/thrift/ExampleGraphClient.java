package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain.BoundingBox;
import org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain.GeoPoint;
import org.apache.tinkerpop.gremlin.language.examples.thrift.mydomain.MyApplication;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

import java.io.IOException;

/*
Inspect the resulting graph in Gremlin Console with:

    graph = TinkerGraph.open();
    reader = GraphSONReader.build().create()
    stream = new FileInputStream("/tmp/graphs-over-thrift.json")
    reader.readGraph(stream, graph);
    g = graph.traversal()
 */
public class ExampleGraphClient {
    private static final GraphEncoding encoding = new GraphEncoding(MyApplication.SUPPORTED_ENCODINGS);

    public static void main(String[] args) {
        try {
            TTransport transport;

            transport = new TSocket("localhost", 9090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            ExampleService.Client client = new ExampleService.Client(protocol);

            perform(client);

            transport.close();
        } catch (TException x) {
            x.printStackTrace();
        }
    }

    private static void perform(ExampleService.Client client) throws TException {
        Graph graph = TinkerFactory.createClassic();
        GraphTraversalSource g = graph.traversal();
        g.V(4L).next().property("awesomeness", 1.0);
        g.V(4L).next().property("livesIn",
                new BoundingBox(
                        new GeoPoint(37.8578475f,-122.5817373f, null),
                        new GeoPoint(37.1720048f,-121.6200625f, null)));

        AddGraphResponse response;
        try {
            response = client.addGraph(encoding.fromNativeGraph(graph));
        } catch (IOException e) {
            throw new TException(e);
        }

        System.out.println("resulting graph has " + response.totalVerticesAfterOperation + " vertices and "
            + response.totalEdgesAfterOperation + " edges.");
    }
}