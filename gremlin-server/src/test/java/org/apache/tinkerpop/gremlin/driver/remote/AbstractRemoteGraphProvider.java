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

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.structure.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.ServerTestHelper;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;

import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.remote.RemoteConnection.GREMLIN_REMOTE;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest",
        method = "*",
        reason = "Tests for profile() are not supported for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.LambdaStepTest",
        method = "*",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupTest",
        method = "g_V_both_name_order_byXa_bX_dedup_value",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_name_order_byXa1_b1X_byXb2_a2X",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderTest",
        method = "g_V_order_byXname_a1_b1X_byXname_b2_a2X_name",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SackTest",
        method = "g_withSackXmap__map_cloneX_V_out_out_sackXmap_a_nameX_sack",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_V_withSideEffectXsgX_outEXknowsX_subgraphXsgX_name_capXsgX",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_V_withSideEffectXsgX_repeatXbothEXcreatedX_subgraphXsgX_outVX_timesX5X_name_dedup",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest",
        method = "g_withSideEffectXsgX_V_hasXname_danielXout_capXsgX",
        reason = "Tests that include lambdas are not supported by the test suite for remotes")
@Graph.OptOut(
        test = "org.apache.tinkerpop.gremlin.process.traversal.step.OrderabilityTest",
        method = "g_inject_order_with_unknown_type",
        reason = "Tests that inject a generic Java Object are not supported by the test suite for remotes")
public abstract class AbstractRemoteGraphProvider extends AbstractGraphProvider implements AutoCloseable {
    private final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(RemoteGraph.class);
    }};

    private static GremlinServer server;
    private final Map<String, RemoteGraph> remoteCache = new HashMap<>();
    private final Cluster cluster;
    private final Client client;
    private final boolean useComputer;


    public AbstractRemoteGraphProvider(final Cluster cluster) {
        this(cluster, false);
    }

    public AbstractRemoteGraphProvider(final Cluster cluster, final boolean useComputer) {
        this.cluster = cluster;
        this.client = this.cluster.connect();
        this.useComputer = useComputer;
        try {
            startServer();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            cluster.close();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

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
                k -> RemoteGraph.open(new DriverRemoteConnection(cluster, config), config));
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {
        final String serverGraphName = getServerGraphName(loadGraphWith);

        final Supplier<Graph> graphGetter = () -> server.getServerGremlinExecutor().getGraphManager().getGraph(serverGraphName);
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, RemoteGraph.class.getName());
            put(RemoteConnection.GREMLIN_REMOTE_CONNECTION_CLASS, DriverRemoteConnection.class.getName());
            put(DriverRemoteConnection.GREMLIN_REMOTE_DRIVER_SOURCENAME, "g" + serverGraphName);
            put("clusterConfiguration.port", TestClientFactory.PORT);
            put("clusterConfiguration.hosts", "localhost");
            put(GREMLIN_REMOTE + "attachment", graphGetter);
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
        assert graph instanceof RemoteGraph;

        // ensure that traversal is created using withRemote() rather than just using RemoteGraph. withRemote() is
        // the appropriate way for users to create a remote traversal. RemoteGraph has been deprecated for users
        // concerns and will be likely relegated to the test module so that OptOut can continue to work and we can
        // full execute the process tests. we should be able to clean this up considerably when RemoteGraph can be
        // moved with breaking change.
        final GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(((RemoteGraph) graph).getConnection());

        if (useComputer) {
            final int state = TestHelper.RANDOM.nextInt(3);
            switch (state) {
                case 0:
                    return g.withComputer();
                case 1:
                    return g.withComputer(Computer.compute(TinkerGraphComputer.class));
                case 2:
                    return g.withComputer(Computer.compute(TinkerGraphComputer.class).workers(TestHelper.RANDOM.nextInt(AVAILABLE_PROCESSORS) + 1));
                default:
                    throw new IllegalStateException("This state should not have occurred: " + state);
            }
        }

        return g;
    }

    public static Cluster.Builder createClusterBuilder(final Serializers serializer) {
        // match the content length in the server yaml
        return TestClientFactory.build().maxContentLength(1000000).serializer(serializer);
    }

    public static void startServer() throws Exception {
        final InputStream stream = GremlinServer.class.getResourceAsStream("gremlin-server-integration.yaml");
        final Settings settings = Settings.read(stream);
        ServerTestHelper.rewritePathsInGremlinServerSettings(settings);

        settings.maxContentLength = 1024000;
        settings.maxChunkSize = 1024000;

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
