package org.apache.tinkerpop.gremlin.language.examples.thrift;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class ExampleGraphServer {

    public static AddGraphHandler handler;

    public static ExampleService.Processor processor;

    public static void main(String[] args) {
        try {
            handler = new AddGraphHandler();
            processor = new ExampleService.Processor(handler);

            Runnable simple = () -> simple(processor);

            new Thread(simple).start();
        } catch (Exception x) {
            x.printStackTrace();
        }
    }

    public static void simple(ExampleService.Processor processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the graph server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
