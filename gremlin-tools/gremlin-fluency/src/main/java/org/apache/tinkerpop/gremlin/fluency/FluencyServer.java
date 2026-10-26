/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.features.StepDefinition;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An embedded Gremlin Server that lets tool-using setups, such as an agent with gremlin-mcp, query the graph of the
 * scenario they are attempting. Each graph is served as a traversal source with a neutral name, {@code g1},
 * {@code g2} and so on, because the source name reaches the agent, for example in server errors, and the name of a
 * test graph would tell it which graph it is working on. The preloaded graphs are copies of the scoring graphs,
 * served with {@link ReadOnlyStrategy} to guard against a stray write. The empty graph is writable, and
 * {@link #prepare(IntentScenario, int)} resets it before each attempt to the state the scenario starts from. Attempts
 * made at the same time each run in their own lane, and each lane has its own empty graph, so that one attempt cannot
 * reset or change the graph another is working on.
 */
final class FluencyServer implements AutoCloseable {
    static final String HOST = "localhost";

    private final GremlinServer server;
    private final int port;
    private final Map<String, AbstractTinkerGraph> graphs = new LinkedHashMap<>();
    private final Map<String, String> sources = new LinkedHashMap<>();
    private final List<AbstractTinkerGraph> emptyGraphs = new ArrayList<>();
    private final List<String> emptySources = new ArrayList<>();

    private FluencyServer(final GremlinServer server, final int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Starts a server on a free port that serves the named graphs, named as in the feature files, for one lane.
     */
    static FluencyServer start(final Collection<String> graphNames) throws Exception {
        return start(graphNames, 1);
    }

    /**
     * Starts a server on a free port that serves the named graphs, named as in the feature files, with an empty graph
     * for each of the given number of lanes.
     */
    static FluencyServer start(final Collection<String> graphNames, final int lanes) throws Exception {
        if (lanes < 1) throw new IllegalArgumentException("lanes must be at least 1");
        final Settings settings = new Settings();
        settings.host = HOST;
        settings.port = freePort();
        final FluencyServer fluencyServer = new FluencyServer(new GremlinServer(settings), settings.port);
        final GraphManager graphManager = fluencyServer.server.start().join().getGraphManager();

        int number = 0;
        for (String graphName : graphNames) {
            if (graphName.equals("empty")) {
                for (int lane = 0; lane < lanes; lane++) {
                    final AbstractTinkerGraph graph = FluencyWorld.emptyGraph();
                    number++;
                    fluencyServer.emptyGraphs.add(graph);
                    fluencyServer.emptySources.add("g" + number);
                    graphManager.putGraph("graph" + number, graph);
                    graphManager.putTraversalSource("g" + number, graph.traversal());
                }
            } else {
                final AbstractTinkerGraph graph = Perturber.copy(FluencyWorld.graph(graphName));
                number++;
                fluencyServer.graphs.put(graphName, graph);
                fluencyServer.sources.put(graphName, "g" + number);
                graphManager.putGraph("graph" + number, graph);
                graphManager.putTraversalSource("g" + number, graph.traversal().withStrategies(ReadOnlyStrategy.instance()));
            }
        }
        return fluencyServer;
    }

    /**
     * Where the scenario's graph is served for the first lane.
     */
    String endpoint(final String graphName) {
        return endpoint(graphName, 0);
    }

    /**
     * Where the scenario's graph is served for a lane, as {@code host:port/source}, which is the form gremlin-mcp's
     * {@code GREMLIN_MCP_ENDPOINT} takes. Only the empty graph differs from lane to lane.
     */
    String endpoint(final String graphName, final int lane) {
        final String source;
        if (graphName.equals("empty") && !emptySources.isEmpty())
            source = emptySources.get(checkLane(lane));
        else if (graphs.containsKey(graphName))
            source = sources.get(graphName);
        else
            throw new IllegalArgumentException("The server does not serve the " + graphName + " graph");
        return HOST + ":" + port + "/" + source;
    }

    /**
     * Readies the scenario's graph for an attempt in the first lane.
     */
    void prepare(final IntentScenario scenario) {
        prepare(scenario, 0);
    }

    /**
     * Readies the scenario's graph for an attempt in a lane. The lane's empty graph is cleared, as the language
     * variant test harnesses clear it, and then given the scenario's initializer, if it has one, so an agent that
     * inspects it sees the state the test starts from.
     */
    void prepare(final IntentScenario scenario, final int lane) {
        if (!"empty".equals(scenario.getGraph())) return;

        final AbstractTinkerGraph graph = emptyGraphs.get(checkLane(lane));
        graph.traversal().V().drop().iterate();
        for (ScenarioStep step : scenario.getSteps()) {
            if (step.getText().equals("the graph initializer of")) {
                final StepDefinition steps = new StepDefinition(graphData -> graph.traversal());
                steps.givenTheXGraph("empty");
                steps.theGraphInitializerOf(step.getDocString());
            }
        }
    }

    @Override
    public void close() {
        server.stop().join();
    }

    private int checkLane(final int lane) {
        if (lane < 0 || lane >= emptyGraphs.size())
            throw new IllegalArgumentException(String.format("lane %d is not one of the %d lanes", lane, emptyGraphs.size()));
        return lane;
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
