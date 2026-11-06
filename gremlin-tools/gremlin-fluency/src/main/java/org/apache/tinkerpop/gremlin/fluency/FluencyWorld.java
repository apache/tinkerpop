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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerDegreeCentralityFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerTextSearchFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.AbstractTinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerMemoryGraph;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * The {@link World} that candidates are scored in. It serves the test graphs from TinkerGraph as the TinkerGraph
 * feature tests do, and a fresh empty graph for each scenario that asks for one, so mutating scenarios stay
 * isolated. Graphs are built on first use and shared by every scenario after that, so they are served with
 * {@link ReadOnlyStrategy} to keep a candidate that writes from changing the graph every later scenario reads.
 */
public class FluencyWorld implements World {
    private static final Map<GraphData, AbstractTinkerGraph> graphs = new EnumMap<>(GraphData.class);

    @Override
    public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
        if (null == graphData)
            return emptyGraph().traversal();

        synchronized (graphs) {
            return graphs.computeIfAbsent(graphData, gd -> registerTestServices(create(gd))).traversal().
                    withStrategies(ReadOnlyStrategy.instance());
        }
    }

    /**
     * Builds the named graph ahead of scoring so that loading it, which takes a few seconds for air-routes, is not
     * counted against the first scenario's timeout. Graph names are those of the feature files.
     */
    static void load(final String graphName) {
        if (graphName.equals("empty")) return;
        graph(graphName);
    }

    /**
     * The shared instance of the named graph, built on first use, or a new empty graph for {@code empty}. Callers
     * must not modify a shared instance.
     */
    static AbstractTinkerGraph graph(final String graphName) {
        if (graphName.equals("empty")) return emptyGraph();

        // graph names in the feature files drop the underscore of multi-word GraphData names (e.g. airroutes)
        final GraphData graphData = Stream.of(GraphData.values()).
                filter(gd -> gd.name().replace("_", "").equalsIgnoreCase(graphName)).
                findFirst().orElseThrow(() -> new IllegalArgumentException("No graph named " + graphName));
        synchronized (graphs) {
            return graphs.computeIfAbsent(graphData, gd -> registerTestServices(create(gd)));
        }
    }

    /**
     * A new empty graph configured as the one served to scenarios that start from the empty graph.
     */
    static AbstractTinkerGraph emptyGraph() {
        return registerTestServices(TinkerMemoryGraph.open(getNumberIdManagerConfiguration()));
    }

    private static AbstractTinkerGraph create(final GraphData graphData) {
        switch (graphData) {
            case CLASSIC:
                return TinkerFactory.createClassic();
            case MODERN:
                return TinkerFactory.createModern();
            case CREW:
                return TinkerFactory.createTheCrew();
            case SINK:
                return TinkerFactory.createKitchenSink();
            case GRATEFUL:
                return TinkerFactory.createGratefulDead();
            case ZOO:
                return TinkerFactory.createTheZoo();
            case AIR_ROUTES:
                return TinkerFactory.createAirRoutes();
            default:
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
        }
    }

    private static Configuration getNumberIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, AbstractTinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, AbstractTinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, AbstractTinkerGraph.DefaultIdManager.LONG.name());
        return conf;
    }

    static AbstractTinkerGraph registerTestServices(final AbstractTinkerGraph graph) {
        graph.getServiceRegistry().registerService(new TinkerTextSearchFactory(graph));
        graph.getServiceRegistry().registerService(new TinkerDegreeCentralityFactory(graph));
        return graph;
    }
}
