package examples;

import org.apache.tinkerpop.gremlin.driver.Channelizer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Connections {
    public static void main(String[] args) throws Exception {
        withEmbedded();
        withRemote();
        withConf();
        withCluster();
        withSerializer();
    }

    // Creating an embedded graph
    private static void withEmbedded() throws Exception {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = traversal().withEmbedded(graph);
        g.close();
    }

    // Connecting to the server
    private static void withRemote() throws Exception {
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));
        g.close();
    }

    // Connecting to the server with a configurations file
    private static void withConf() throws Exception {
        GraphTraversalSource g = traversal().withRemote("gremlin-driver/src/main/java/examples/conf/remote-graph.properties");
        g.close();
    }

    // Connecting and customizing configurations with a cluster
    // See reference/#gremlin-java-configuration for full list of configurations
    private static void withCluster() throws Exception {
        Cluster cluster = Cluster.build("localhost").
                channelizer(Channelizer.HttpChannelizer.class).
                keepAliveInterval(180000).
                maxConnectionPoolSize(8).
                path("/gremlin").
                port(8182).
                serializer(new GraphBinaryMessageSerializerV1()).
                create();
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        g.close();
    }

    // Connecting and specifying a serializer
    private static void withSerializer() throws Exception {
        IoRegistry registry = new FakeIoRegistry(); // an IoRegistry instance exposed by a specific graph provider
        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build().addRegistry(registry).create();
        MessageSerializer serializer = new GraphBinaryMessageSerializerV1(typeSerializerRegistry);
        Cluster cluster = Cluster.build("localhost").
                serializer(serializer).
                create();
        Client client = cluster.connect();
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));
        g.close();
    }

    public static class FakeIoRegistry extends AbstractIoRegistry {}
}
