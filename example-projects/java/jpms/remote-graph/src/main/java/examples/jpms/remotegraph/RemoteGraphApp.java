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
package examples.jpms.remotegraph;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.DefaultGraphProvider;

import java.io.IOException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Gets a {@code subgraph()} result from Gremlin Server. Deserializing a {@link Graph} result needs a
 * {@link DefaultGraphProvider}, which the TinkerGraph module provides when it is on the module path. Run with
 * {@code -Dexamples.jpms.expectNoGraphProvider=true} and without TinkerGraph on the module path, the application
 * instead checks that the result fails with the message that explains how to add TinkerGraph. The server is found
 * through the {@code gremlin.server.host} and {@code gremlin.server.port} system properties and must host the
 * "modern" graph as {@code g}. Any failed check throws, so the process exits with a non-zero status.
 */
public final class RemoteGraphApp {

    private static final String TINKERGRAPH_MODULE = "org.apache.tinkerpop.gremlin.tinkergraph";

    private RemoteGraphApp() {}

    public static void main(final String[] args) throws Exception {
        final String host = System.getProperty("gremlin.server.host", "localhost");
        final int port = Integer.parseInt(System.getProperty("gremlin.server.port", "8182"));
        final boolean expectNoGraphProvider = Boolean.getBoolean("examples.jpms.expectNoGraphProvider");

        final boolean tinkerGraphPresent = ModuleLayer.boot().findModule(TINKERGRAPH_MODULE).isPresent();
        if (tinkerGraphPresent == expectNoGraphProvider)
            throw new IllegalStateException("the " + TINKERGRAPH_MODULE + " module was " +
                    (tinkerGraphPresent ? "" : "not ") + "resolved, which this run doesn't expect");

        final Cluster cluster = Cluster.build(host).port(port).create();
        try {
            final GraphTraversalSource g = traversal().with(DriverRemoteConnection.using(cluster, "g"));
            try {
                if (expectNoGraphProvider)
                    checkSubgraphFailsWithoutProvider(g);
                else
                    checkSubgraph(g);
            } finally {
                g.close();
            }
        } finally {
            cluster.close();
        }
        System.out.println("remote graph example completed");
    }

    private static Graph subgraphOfKnows(final GraphTraversalSource g) {
        return (Graph) g.E().hasLabel("knows").subgraph("sg").cap("sg").next();
    }

    private static void checkSubgraph(final GraphTraversalSource g) throws Exception {
        try (Graph subgraph = subgraphOfKnows(g)) {
            final String module = subgraph.getClass().getModule().getName();
            if (!TINKERGRAPH_MODULE.equals(module))
                throw new IllegalStateException("the subgraph() result should be a TinkerGraph but was a " +
                        subgraph.getClass().getName() + " from module " + module);

            final GraphTraversalSource sg = subgraph.traversal();
            check(3L, sg.V().count().next(), "vertex count of the subgraph");
            check(2L, sg.E().count().next(), "edge count of the subgraph");
        }
    }

    private static void checkSubgraphFailsWithoutProvider(final GraphTraversalSource g) {
        try {
            subgraphOfKnows(g);
        } catch (Exception ex) {
            // subgraph() on the server reports the same message as an IllegalStateException, so only accept the
            // IOException from deserializing the Graph result on this side
            for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
                if (cause instanceof IOException &&
                        String.valueOf(cause.getMessage()).contains(DefaultGraphProvider.NO_PROVIDER_MESSAGE)) {
                    System.out.println("subgraph() failed without TinkerGraph on the module path, as expected: " +
                            cause.getMessage());
                    return;
                }
            }
            throw new IllegalStateException("subgraph() failed, but not with an IOException from deserializing the " +
                    "Graph result because no DefaultGraphProvider was found: " + ex, ex);
        }
        throw new IllegalStateException("subgraph() was expected to fail without TinkerGraph on the module path");
    }

    private static void check(final Object expected, final Object actual, final String what) {
        if (!expected.equals(actual))
            throw new IllegalStateException(what + ": expected " + expected + " but was " + actual);
    }
}
