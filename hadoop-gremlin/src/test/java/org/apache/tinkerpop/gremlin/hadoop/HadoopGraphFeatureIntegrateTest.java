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
package org.apache.tinkerpop.gremlin.hadoop;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.guice.GuiceFactory;
import io.cucumber.guice.InjectorSource;
import io.cucumber.java.Scenario;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.javatuples.Pair;
import org.junit.AssumptionViolatedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;

@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @RemoteOnly and not @GraphComputerOnly and not @TinkerServiceRegistry and not @GraphComputerVerificationElementSupported",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = GuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = {"progress", "junit:target/cucumber.xml"})
public class HadoopGraphFeatureIntegrateTest {
    private static final String skipReasonLength = "Hadoop-Gremlin is OLAP-oriented and for OLTP operations, linear-scan joins are required. This particular tests takes many minutes to execute.";

    private static final List<Pair<String, String>> skip = new ArrayList<Pair<String,String>>() {{
       add(Pair.with("g_V_both_both_count", skipReasonLength));
        add(Pair.with("g_V_repeatXoutX_timesX3X_count", skipReasonLength));
        add(Pair.with("g_V_repeatXoutX_timesX8X_count", skipReasonLength));
        add(Pair.with("g_V_repeatXoutX_timesX5X_asXaX_outXwrittenByX_asXbX_selectXa_bX_count", skipReasonLength));
        add(Pair.with("g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_dX_whereXc_sungBy_dX_whereXd_hasXname_GarciaXX", skipReasonLength));
        add(Pair.with("g_V_matchXa_0sungBy_b__a_0writtenBy_c__b_writtenBy_d__c_sungBy_d__d_hasXname_GarciaXX", skipReasonLength));
        add(Pair.with("g_V_matchXa_0sungBy_b__a_0sungBy_c__b_writtenBy_d__c_writtenBy_e__d_hasXname_George_HarisonX__e_hasXname_Bob_MarleyXX", skipReasonLength));
        add(Pair.with("g_V_matchXa_hasXname_GarciaX__a_0writtenBy_b__a_0sungBy_bX", skipReasonLength));
        add(Pair.with("g_V_hasLabelXsongX_groupXaX_byXnameX_byXproperties_groupCount_byXlabelXX_out_capXaX", skipReasonLength));
        add(Pair.with("g_V_outXfollowedByX_group_byXsongTypeX_byXbothE_group_byXlabelX_byXweight_sumXX", skipReasonLength));
        add(Pair.with("g_V_repeatXbothXfollowedByXX_timesX2X_group_byXsongTypeX_byXcountX", skipReasonLength));
        add(Pair.with("g_V_repeatXbothXfollowedByXX_timesX2X_groupXaX_byXsongTypeX_byXcountX_capXaX", skipReasonLength));
        add(Pair.with("g_V_matchXa_followedBy_count_isXgtX10XX_b__a_0followedBy_count_isXgtX10XX_bX_count", skipReasonLength));
    }};

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(HadoopGraphWorld.class);
        }
    }

    public static class HadoopGraphWorld implements World {

        private static final HadoopGraph modern = HadoopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.MODERN)));
        private static final HadoopGraph classic = HadoopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.CLASSIC)));
        private static final HadoopGraph crew = HadoopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.CREW)));
        private static final HadoopGraph sink = HadoopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.SINK)));
        private static final HadoopGraph grateful = HadoopGraph.open(new MapConfiguration(getBaseConfiguration(GraphData.GRATEFUL)));

        static {
            readIntoGraph(modern, GraphData.MODERN);
            readIntoGraph(classic, GraphData.CLASSIC);
            readIntoGraph(crew, GraphData.CREW);
            readIntoGraph(sink, GraphData.SINK);
            readIntoGraph(grateful, GraphData.GRATEFUL);
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final GraphData graphData) {
            if (null == graphData)
                throw new AssumptionViolatedException("HadoopGraph does not support graph mutations");
            else if (graphData == GraphData.CLASSIC)
                return classic.traversal();
            else if (graphData == GraphData.CREW)
                return crew.traversal();
            else if (graphData == GraphData.MODERN)
                return modern.traversal();
            else if (graphData == GraphData.SINK)
                return sink.traversal();
            else if (graphData == GraphData.GRATEFUL)
                return grateful.traversal();
            else
                throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
        }

        @Override
        public void beforeEachScenario(final Scenario scenario) {
            final Optional<Pair<String,String>> skipped = skip.stream().
                    filter(s -> s.getValue0().equals(scenario.getName())).findFirst();
            if (skipped.isPresent())
                throw new AssumptionViolatedException(skipped.get().getValue1());
        }

        private static void readIntoGraph(final Graph graph, final GraphData graphData) {
            ((HadoopGraph) graph).configuration().setInputLocation(TestFiles.getInputLocation(graphData, false));
        }

        private static String getWorkingDirectory() {
            return TestHelper.makeTestDataDirectory(HadoopGraphFeatureIntegrateTest.class, "graph-provider-data");
        }

        private static Map<String, Object> getBaseConfiguration(final GraphData graphData) {
            return new HashMap<String, Object>() {{
                put(Graph.GRAPH, HadoopGraph.class.getName());
                put(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
                put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
                put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, getWorkingDirectory());
                put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
            }};
        }

        @Override
        public boolean useParametersLiterally() {
            return false;
        }
    }

    public static final class WorldInjectorSource implements InjectorSource {
        @Override
        public Injector getInjector() {
            return Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule());
        }
    }

}
