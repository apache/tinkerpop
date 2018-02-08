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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.ServerTestHelper;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RemoteGraphProvider extends AbstractGraphProvider implements AutoCloseable {
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(RemoteGraph.class);
    }};

    private static GremlinServer server;
    private final Map<String, RemoteGraph> remoteCache = new HashMap<>();
    private final Cluster cluster = TestClientFactory.open();
    private final Client client = cluster.connect();

    public RemoteGraphProvider() {
        try {
            startServer();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            stopServer();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Graph openTestGraph(final Configuration config) {
        final String serverGraphName = config.getString(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME);
        return remoteCache.computeIfAbsent(serverGraphName,
                k -> RemoteGraph.open(new DriverRemoteConnection(cluster, config)));
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        final String serverGraphName = getServerGraphName(loadGraphWith);

        final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph(serverGraphName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RemoteGraph.class.getName());
            put(RemoteGraph.GREMLIN_REMOTE_GRAPH_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
            put(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g" + serverGraphName);
            put("clusterConfiguration.port", TestClientFactory.PORT);
            put("clusterConfiguration.hosts", "localhost");
            put("hidden.for.testing.only", graphGetter);
        }};
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        // doesn't bother to clear grateful/sink because i don't believe that ever gets mutated - read-only
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

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        // ensure that traversal is created using withRemote() rather than just using RemoteGraph. withRemote() is
        // the appropriate way for users to create a remote traversal. RemoteGraph has been deprecated for users
        // concerns and will be likely relegated to the test module so that OptOut can continue to work and we can
        // full execute the process tests. we should be able to clean this up considerably when RemoteGraph can be
        // moved with breaking change.
        return super.traversal(graph).withRemote(((RemoteGraph) graph).getConnection());
    }

    public static void startServer() throws Exception {
        final InputStream stream = RemoteGraphProvider.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);

        settings.maxContentLength = 1024000;
        settings.maxChunkSize =1024000;

        server = new GremlinServer(settings);

        server.start().get(100, TimeUnit.SECONDS);
    }

    public static void stopServer() throws Exception {
        server.stop().get(100, TimeUnit.SECONDS);
        server = null;
    }

    private static String getServerGraphName(final LoadGraphWith.GraphData loadGraphWith) {
        final String serverGraphName;

        if (null == loadGraphWith) return "graph";

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
            case SINK:
                serverGraphName = "sink";
                break;
            default:
                serverGraphName = "graph";
                break;
        }
        return serverGraphName;
    }
}
