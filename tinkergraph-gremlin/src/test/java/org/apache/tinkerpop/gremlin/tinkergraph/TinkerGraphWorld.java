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
package org.apache.tinkerpop.gremlin.tinkergraph;

import io.cucumber.java.Scenario;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerDegreeCentralityFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.services.TinkerTextSearchFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * The {@link World} implementation for TinkerGraph that provides the {@link GraphTraversalSource} instances required
 * by the Gherkin test suite.
 */
public class TinkerGraphWorld implements World {
    private static final TinkerGraph modern = registerTestServices(TinkerFactory.createModern());
    private static final TinkerGraph classic = registerTestServices(TinkerFactory.createClassic());
    private static final TinkerGraph crew = registerTestServices(TinkerFactory.createTheCrew());
    private static final TinkerGraph sink = registerTestServices(TinkerFactory.createKitchenSink());
    private static final TinkerGraph grateful = registerTestServices(TinkerFactory.createGratefulDead());

    private static TinkerGraph registerTestServices(final TinkerGraph graph) {
        graph.getServiceRegistry().registerService(new TinkerTextSearchFactory(graph));
        graph.getServiceRegistry().registerService(new TinkerDegreeCentralityFactory(graph));
        return graph;
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        if (null == graphData)
            return registerTestServices(TinkerGraph.open(getNumberIdManagerConfiguration())).traversal();
        else if (graphData == LoadGraphWith.GraphData.CLASSIC)
            return classic.traversal();
        else if (graphData == LoadGraphWith.GraphData.CREW)
            return crew.traversal();
        else if (graphData == LoadGraphWith.GraphData.MODERN)
            return modern.traversal();
        else if (graphData == LoadGraphWith.GraphData.SINK)
            return sink.traversal();
        else if (graphData == LoadGraphWith.GraphData.GRATEFUL)
            return grateful.traversal();
        else
            throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
        return ".." + File.separator + pathToFileFromGremlin;
    }

    private static Configuration getNumberIdManagerConfiguration() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, TinkerGraph.DefaultIdManager.INTEGER.name());
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, TinkerGraph.DefaultIdManager.LONG.name());
        return conf;
    }

    /**
     * Enables the storing of {@code null} property values when testing.
     */
    public static class NullWorld extends TinkerGraphWorld {

        @Override
        public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
            if (graphData != null)
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());

            final Configuration conf = TinkerGraphWorld.getNumberIdManagerConfiguration();
            conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
            return TinkerGraph.open(conf).traversal();
        }
    }

    /**
     * Turns on {@link GraphComputer} when testing.
     */
    public static class ComputerWorld extends TinkerGraphWorld {
        private static final Random RANDOM = TestHelper.RANDOM;

        private static final List<String> TAGS_TO_IGNORE = Arrays.asList(
                "@StepDrop",
                "@StepInject",
                "@StepV",
                "@GraphComputerVerificationOneBulk",
                "@GraphComputerVerificationStrategyNotSupported",
                "@GraphComputerVerificationMidVNotSupported",
                "@GraphComputerVerificationInjectionNotSupported",
                "@GraphComputerVerificationStarGraphExceeded",
                "@GraphComputerVerificationReferenceOnly",
                "@TinkerServiceRegistry");

        private static final List<String> SCENARIOS_TO_IGNORE = Arrays.asList(
                "g_V_group_byXoutE_countX_byXnameX",
                "g_V_asXvX_mapXbothE_weight_foldX_sumXlocalX_asXsX_selectXv_sX_order_byXselectXsX_descX",
                "g_V_hasXlangX_groupXaX_byXlangX_byXnameX_out_capXaX",
                "g_withStrategiesXProductiveByStrategyX_V_group_byXageX",
                "g_V_order_byXoutE_count_descX",
                "g_V_both_both_dedup_byXoutE_countX_name",
                "g_V_mapXbothE_weight_foldX_order_byXsumXlocalX_descX",
                "g_V_hasLabelXsoftwareX_order_byXnameX_index_withXmapX",
                "g_V_order_byXname_descX_barrier_dedup_age_name");

        @Override
        public void beforeEachScenario(final Scenario scenario) {
            final List<String> ignores = TAGS_TO_IGNORE.stream().filter(t -> scenario.getSourceTagNames().contains(t)).collect(Collectors.toList());
            if (!ignores.isEmpty())
                throw new AssumptionViolatedException(String.format("This scenario is not supported with GraphComputer: %s", ignores));

            // the following needs some further investigation.........may need to improve the definition of result
            // equality with map<list>
            final String scenarioName = scenario.getName();
            if (SCENARIOS_TO_IGNORE.contains(scenarioName))
                throw new AssumptionViolatedException("There are some internal ordering issues with result where equality is not required but is being enforced");
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
            if (null == graphData)
                throw new AssumptionViolatedException("GraphComputer does not support mutation");

            return super.getGraphTraversalSource(graphData).withStrategies(VertexProgramStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
                put(VertexProgramStrategy.WORKERS, Runtime.getRuntime().availableProcessors());
                put(VertexProgramStrategy.GRAPH_COMPUTER, RANDOM.nextBoolean() ?
                        GraphComputer.class.getCanonicalName() :
                        TinkerGraphComputer.class.getCanonicalName());
            }})));
        }
    }
}
