package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;

public class ExampleGraphClient {

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
        g.V(4L).next().property("awesomenessLevel", 1.0);

        AddGraphResponse response = client.addGraph(GraphThriftUtils.fromNativeGraph(graph));
        System.out.println("resulting graph has " + response.totalVerticesAfterOperation + " vertices and "
            + response.totalEdgesAfterOperation + " edges.");
    }
}