package example;/*
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

// Common imports as listed at reference/#gremlin-java-imports
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Order.*;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Pop.*;
import static org.apache.tinkerpop.gremlin.process.traversal.SackFunctions.*;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.*;
import static org.apache.tinkerpop.gremlin.process.traversal.TextP.*;
import static org.apache.tinkerpop.gremlin.structure.Column.*;
import static org.apache.tinkerpop.gremlin.structure.Direction.*;
import static org.apache.tinkerpop.gremlin.structure.T.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
public class Example {

    public static void main(String[] args) throws Exception {
        // Creating an embedded graph
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = traversal().withEmbedded(graph);

        // Connecting to the server
        g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8192, "g"));

        // Connecting to the server with a configurations file
        g = traversal().withRemote("gremlin-driver/src/main/java/example/conf/remote-graph.properties");

        // Connecting and customizing configurations with a cluster
        // See reference/#gremlin-java-configuration for full list of configurations
        Cluster cluster = Cluster.build().
                channelizer("org.apache.tinkerpop.gremlin.driver.Channelizer$HttpChannelizer").
                keepAliveInterval(180000).
                maxConnectionPoolSize(8).
                path("/gremlin").
                port(8192).
                serializer(new GraphBinaryMessageSerializerV1()).
                create();
        g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        // Connecting and specifying a serializer
        IoRegistry registry = new FakeIoRegistry(); // an IoRegistry instance exposed by a specific graph provider
        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build().addRegistry(registry).create();
        MessageSerializer serializer = new GraphBinaryMessageSerializerV1(typeSerializerRegistry);
        cluster = Cluster.build().
                serializer(serializer).
                create();
        Client client = cluster.connect();
        g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));

        // Cleanup
        g.close();

        System.out.println(g.V());

    }

    public static class FakeIoRegistry extends AbstractIoRegistry {
    }
}