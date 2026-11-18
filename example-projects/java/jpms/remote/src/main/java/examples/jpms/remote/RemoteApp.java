/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package examples.jpms.remote;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Connects to Gremlin Server with the Java driver from a named module: submits a script, runs a remote traversal
 * through {@link DriverRemoteConnection} and connects again with a {@link Cluster} configured from a YAML file.
 * The server is found
 * through the {@code gremlin.server.host} and {@code gremlin.server.port} system properties and must host the
 * "modern" graph as {@code g}. Any failed check throws, so the process exits with a non-zero status.
 */
public final class RemoteApp {

    private RemoteApp() {}

    public static void main(final String[] args) throws Exception {
        final String host = System.getProperty("gremlin.server.host", "localhost");
        final int port = Integer.parseInt(System.getProperty("gremlin.server.port", "8182"));

        final Cluster cluster = Cluster.build(host).port(port).create();
        try {
            submitScript(cluster);
            runRemoteTraversal(cluster);
        } finally {
            cluster.close();
        }

        connectWithYamlConfiguration(host, port);
        System.out.println("remote example completed");
    }

    private static void submitScript(final Cluster cluster) throws Exception {
        final Client client = cluster.connect().alias("g");
        try {
            check(4L, client.submit("g.V().hasLabel('person').count()").one().getLong(), "person count from a script");
        } finally {
            client.close();
        }
    }

    private static void runRemoteTraversal(final Cluster cluster) throws Exception {
        final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster, "g"));
        try {
            final Set<Object> friends = new HashSet<>(g.V().has("person", "name", "marko").out("knows").values("name").toList());
            check(new HashSet<>(Arrays.asList("vadas", "josh")), friends, "people marko knows");

            final List<Vertex> software = g.V().hasLabel("software").toList();
            check(2, software.size(), "number of software vertices");
            for (final Vertex v : software) {
                check(Set.of("software"), v.labels(), "labels of a returned vertex");
            }
        } finally {
            g.close();
        }
    }

    /**
     * {@link Cluster#build(File)} reads the file with SnakeYAML and populates the driver's public settings classes.
     */
    private static void connectWithYamlConfiguration(final String host, final int port) throws Exception {
        final Path yaml = Files.createTempFile("jpms-remote-", ".yaml");
        try {
            Files.write(yaml, String.join(System.lineSeparator(),
                    "hosts: [" + host + "]",
                    "port: " + port,
                    "connectionPool: {",
                    "  maxConnections: 2,",
                    "  maxWaitForConnection: 6000}").getBytes(StandardCharsets.UTF_8));

            final Cluster cluster = Cluster.build(yaml.toFile()).create();
            try {
                check(port, cluster.getPort(), "port from the YAML file");
                check(2, cluster.maxConnections(), "maxConnections from the YAML file");
                check(6000, cluster.getMaxWaitForConnection(), "maxWaitForConnection from the YAML file");

                final Client client = cluster.connect().alias("g");
                try {
                    check(6L, client.submit("g.V().count()").one().getLong(), "vertex count over the YAML-configured cluster");
                } finally {
                    client.close();
                }
            } finally {
                cluster.close();
            }
        } finally {
            Files.deleteIfExists(yaml);
        }
    }

    private static void check(final Object expected, final Object actual, final String what) {
        if (!expected.equals(actual))
            throw new IllegalStateException(what + ": expected " + expected + " but was " + actual);
    }
}
