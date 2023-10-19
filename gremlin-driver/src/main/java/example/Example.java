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

package example;

// Common imports
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.*;
import static org.apache.tinkerpop.gremlin.structure.T.*;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
public class Example {

    public static void main(String[] args) throws Exception {
        connectionExample();
        basicGremlinExample();
        modernTraversalExample();
    }

    public static void connectionExample() throws Exception {
        // Creating an embedded graph
//        Graph graph = TinkerGraph.open();
//        GraphTraversalSource g = traversal().withEmbedded(graph);

        // Connecting to the server
        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182, "g"));

        // Connecting to the server with a configurations file
//        GraphTraversalSource g = traversal().withRemote("gremlin-driver/src/main/java/example/conf/remote-graph.properties");

        // Connecting and customizing configurations with a cluster
        // See reference/#gremlin-java-configuration for full list of configurations
//        Cluster cluster = Cluster.build().
//            channelizer("org.apache.tinkerpop.gremlin.driver.Channelizer$HttpChannelizer").
//            keepAliveInterval(180000).
//            maxConnectionPoolSize(8).
//            path("/gremlin").
//            port(8182).
//            serializer(new GraphBinaryMessageSerializerV1()).
//            create();
//        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        // Connecting and specifying a serializer
//        IoRegistry registry = new FakeIoRegistry(); // an IoRegistry instance exposed by a specific graph provider
//        TypeSerializerRegistry typeSerializerRegistry = TypeSerializerRegistry.build().addRegistry(registry).create();
//        MessageSerializer serializer = new GraphBinaryMessageSerializerV1(typeSerializerRegistry);
//        Cluster cluster = Cluster.build().
//            serializer(serializer).
//            create();
//        Client client = cluster.connect();
//        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(client, "g"));

        // Cleanup
        g.close();
    }
    public static class FakeIoRegistry extends AbstractIoRegistry {
    }

    private static void basicGremlinExample() {
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = traversal().withEmbedded(graph);

        // Basic Gremlin: adding and retrieving data
        Vertex v1 = g.addV("person").property("name","marko").next();
        Vertex v2 = g.addV("person").property("name","stephen").next();
        Vertex v3 = g.addV("person").property("name","vadas").next();

        // Be sure to use a terminating step like next() or iterate() so that the traversal "executes"
        // Iterate() does not return any data and is used to just generate side-effects (i.e. write data to the database)
        g.V(v1).addE("knows").to(v2).property("weight",0.75).iterate();
        g.V(v1).addE("knows").to(v3).property("weight",0.75).iterate();

        // Retrieve the data from the "marko" vertex
        Object marko = g.V().has("person","name","marko").values("name").next();
        System.out.println("name: " + marko);

        // Find the "marko" vertex and then traverse to the people he "knows" and return their data
        List<Object> peopleMarkoKnows = g.V().has("person","name","marko").out("knows").values("name").toList();
        for (Object person : peopleMarkoKnows) {
            System.out.println("marko knows " + person);
        }
    }

    private static void modernTraversalExample() {
        // Performs basic traversals on the Modern toy graph which can be created using TinkerFactory
        Graph modern = TinkerFactory.createModern();
        GraphTraversalSource g = traversal().withEmbedded(modern);

        List<Edge> e1 = g.V(1).bothE().toList(); // (1)
        List<Edge> e2 = g.V(1).bothE().where(otherV().hasId(2)).toList(); // (2)
        Vertex v1 = g.V(1).next();
        Vertex v2 = g.V(2).next();
        List<Edge> e3 = g.V(v1).bothE().where(otherV().is(v2)).toList(); // (3)
        List<Edge> e4 = g.V(v1).outE().where(inV().is(v2)).toList(); // (4)
        List<Edge> e5 = g.V(1).outE().where(inV().has(id, within(2,3))).toList(); // (5)
        List<Vertex> e6 = g.V(1).out().where(in().hasId(6)).toList(); // (6)

        System.out.println("1: " + e1.toString());
        System.out.println("2: " + e2.toString());
        System.out.println("3: " + e3.toString());
        System.out.println("4: " + e4.toString());
        System.out.println("5: " + e5.toString());
        System.out.println("6: " + e6.toString());

        /*
        1. There are three edges from the vertex with the identifier of "1".
        2. Filter those three edges using the where()-step using the identifier of the vertex returned by otherV() to
           ensure it matches on the vertex of concern, which is the one with an identifier of "2".
        3. Note that the same traversal will work if there are actual Vertex instances rather than just vertex
           identifiers.
        4. The vertex with identifier "1" has all outgoing edges, so it would also be acceptable to use the directional
           steps of outE() and inV() since the schema allows it.
        5. There is also no problem with filtering the terminating side of the traversal on multiple vertices, in this
           case, vertices with identifiers "2" and "3".
        6. There’s no reason why the same pattern of exclusion used for edges with where() can’t work for a vertex
           between two vertices.
        */
    }
}