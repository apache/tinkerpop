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

import java.util.HashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.AbstractIoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Connections {
    public static void main(String[] args) throws Exception {
//        withEmbedded();
//        withRemote();
        withCluster();
//        withSerializer();
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
//        Cluster cluster = Cluster.build("localhost").
//            maxConnectionPoolSize(8).
//            path("/gremlin").
//            port(8182).
//            serializer(new GraphBinaryMessageSerializerV4()).
//            create();

        Cluster cluster = Cluster.build()
                .addContactPoint("g-eodswalp33.us-east-1.neptune-graph-gamma.amazonaws.com")
                .path("/queries")
                .port(443)
                .serializer(Serializers.GRAPHSON_V4)
                .enableSsl(true)
                .maxConnectionPoolSize(1)
                .removeInterceptor(Cluster.SERIALIZER_INTERCEPTOR_NAME) // We don't want the body to be serialized
                .addInterceptor("queries-format", request -> {
                    final RequestMessage rm = (RequestMessage) request.getBody();
                    final Map<String, Object> queriesBody = new HashMap<>();

                    queriesBody.put("query", rm.getGremlin());
                    queriesBody.put("language", "GREMLIN");
                    final Map fields = rm.getFields();
                    if (fields.containsKey("bindings")) queriesBody.put("parameters", fields.get("bindings"));

                    ObjectMapper om = new ObjectMapper();
                    byte[] payload;
                    try {
                        payload = om.writeValueAsBytes(queriesBody);
                    } catch (JsonProcessingException jpe) {
                        throw new RuntimeException(jpe);
                    }
                    request.setBody(payload);

                    Map<String, String> headers = request.headers();
                    headers.remove("accept");
                    headers.remove("accept-encoding");
                    headers.put("content-type", "application/json");
                    headers.put("content-length", String.valueOf(payload.length));

                    return request;
                })
                .auth(Auth.sigv4("us-east-1", "neptune-graph"))
                .enableUserAgentOnConnect(false)
                .create();


        GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        //g.addV().iterate();
        GraphTraversal<Vertex, Vertex> v = g.V();
        GraphTraversal<Vertex, Long> count = v.count();
        Long next = count.next();
        System.out.println("Vertex count: " + next);

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
