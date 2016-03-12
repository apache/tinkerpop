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
package org.apache.tinkerpop.gremlin.remote;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddEdgeTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexTest;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.ServerTestHelper;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.PropertyTest;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RemoteGraphProvider extends AbstractGraphProvider {
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(RemoteGraph.class);
    }};

    private static GremlinServer server;
    private Map<String,RemoteGraph> remoteCache = new HashMap<>();
    private Cluster cluster = Cluster.open();
    private Client client = cluster.connect();

    static {
        try {
            startServer();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        final String serverGraphName = config.getString("connectionGraphName");
        return remoteCache.computeIfAbsent(serverGraphName,
                k -> RemoteGraph.open(new DriverRemoteConnection(cluster, config), TinkerGraph.class));
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        final String serverGraphName = getServerGraphName(loadGraphWith);

        final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraphs().get(serverGraphName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RemoteGraph.class.getName());
            put(RemoteGraph.GREMLIN_SERVERGRAPH_GRAPH_CLASS, TinkerGraph.class.getName());
            put(RemoteGraph.GREMLIN_SERVERGRAPH_SERVER_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
            put("connectionGraphName", serverGraphName);
            put("hidden.for.testing.only", graphGetter);
        }};
    }

    private static String getServerGraphName(LoadGraphWith.GraphData loadGraphWith) {
        final String serverGraphName;
        switch (loadGraphWith) {
            case CLASSIC:
                serverGraphName = "classic";
                break;
            case MODERN:
                serverGraphName = "modern";
                break;
            case GRATEFUL:
                serverGraphName = "grateful";
                break;
            case CREW:
                serverGraphName = "crew";
                break;
            default:
                serverGraphName = "graph";
                break;
        }
        return serverGraphName;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        // doesn't bother to clear grateful because i don't believe that ever gets mutated - read-only
        client.submit("classic.clear();modern.clear();crew.clear();graph.clear();" +
                    "TinkerFactory.generateClassic(classic);" +
                    "TinkerFactory.generateModern(modern);" +
                    "TinkerFactory.generateTheCrew(crew);null").all().get();
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        // server already loads with the all the graph instances for LoadGraphWith
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    public static void startServer() throws Exception {
        final InputStream stream = RemoteGraphProvider.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);
        server = new GremlinServer(settings);

        server.start().join();
    }

    public static void stopServer() throws Exception {
        server.stop().join();
        server = null;
    }
}
