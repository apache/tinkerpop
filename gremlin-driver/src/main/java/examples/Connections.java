/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package examples;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Connections {
    public static void main(String[] args) throws Exception {
        withEmbedded();
        withRemote();
        withCluster();
        withSerializer();
    }

    // Creating an embedded graph
    private static void withEmbedded() throws Exception {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = traversal().withEmbedded(graph);

        g.addV().iterate();
        long count = g.V().count().next();
        System.out.println("Vertex count: " + count);

        g.close();
    }

    // Connecting to the server
    private static void withRemote() throws Exception {
        Cluster cluster = Cluster.build("localhost").port(8182).create();
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        // Drop existing vertices
        g.V().drop().iterate();

        // Simple query to verify connection
        g.addV().iterate();
        long count = g.V().count().next();
        System.out.println("Vertex count: " + count);

        // Cleanup
        cluster.close();
        g.close();
    }

    // Connecting and customizing configurations with a cluster
    // See reference/#gremlin-java-configuration for full list of configurations
    private static void withCluster() throws Exception {
        Cluster cluster = Cluster.build("localhost").
            maxConnectionPoolSize(8).
            path("/gremlin").
            port(8182).
            serializer(new GraphBinaryMessageSerializerV4()).
            create();
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        g.addV().iterate();
        long count = g.V().count().next();
        System.out.println("Vertex count: " + count);

        cluster.close();
        g.close();
    }

    // Connecting and specifying a serializer
    private static void withSerializer() throws Exception {
        IoRegistry registry = new FakeIoRegistry(); // an IoRegistry instance exposed by a specific graph provider
        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build().addRegistry(registry).create();
        MessageSerializer serializer = new GraphBinaryMessageSerializerV4(typeSerializerRegistry);
        Cluster cluster = Cluster.build("localhost").
            serializer(serializer).
            create();
        Client client = cluster.connect();
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));

        g.addV().iterate();
        long count = g.V().count().next();
        System.out.println("Vertex count: " + count);

        cluster.close();
        g.close();
    }

    public static class FakeIoRegistry extends AbstractIoRegistry {}
}
