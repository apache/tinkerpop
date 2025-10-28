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

import io.cucumber.java.Scenario;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.util.ser.Serializers;
import org.apache.tinkerpop.gremlin.features.World;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.junit.AssumptionViolatedException;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The abstract {@link World} implementation for driver/server that provides the {@link GraphTraversalSource} instances
 * required by the Gherkin test suite. A new cluster and client is injected per scenario. To reduce runtime of the
 * tests, the same server is used for every scenario.
 */
public abstract class RemoteWorld implements World {
    private final Cluster cluster;

    /**
     * Helper method to create a test cluster based on the type of serializer. Can be used by implementations to help
     * construct a RemoteWorld.
     */
    public static Cluster createTestCluster(final Serializers serializer) {
        return TestClientFactory.build().serializer(serializer).create();
    }

    public RemoteWorld(Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public void afterEachScenario() {
        cluster.close();
    }

    @Override
    public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
        String remoteTraversalSource = "g"; // these names are from gremlin-server-integration.yaml
        final Client client = cluster.connect();

        if (null == graphData) {
            try { // Clear data before run because tests are allowed to modify data for the empty graph.
                client.submit("graph.clear();").all().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            remoteTraversalSource = "ggraph";
        } else {
            switch (graphData) {
                case CLASSIC:
                    remoteTraversalSource = "gclassic";
                    break;
                case CREW:
                    remoteTraversalSource = "gcrew";
                    break;
                case MODERN:
                    remoteTraversalSource = "gmodern";
                    break;
                case SINK:
                    remoteTraversalSource = "gsink";
                    break;
                case GRATEFUL:
                    remoteTraversalSource = "ggrateful";
                    break;
                default:
                    throw new UnsupportedOperationException("GraphData not supported: " + graphData.name());
            }
        }

        return AnonymousTraversalSource.traversal().withRemote(DriverRemoteConnection.using(client, remoteTraversalSource));
    }

    @Override
    public String changePathToDataFile(final String pathToFileFromGremlin) {
        return ".." + File.separator + pathToFileFromGremlin;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * The abstract {@link World} implementation for driver/server that provides the {@link GraphTraversalSource}
     * instances that include the VertexProgramStrategy.
     */
    public abstract static class RemoteComputerWorld extends RemoteWorld {

        private final int AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors();

        public RemoteComputerWorld(Cluster cluster) {
            super(cluster);
        }

        @Override
        public GraphTraversalSource getGraphTraversalSource(final LoadGraphWith.GraphData graphData) {
            if (null == graphData) throw new AssumptionViolatedException("GraphComputer does not support mutation");

            final int state = TestHelper.RANDOM.nextInt(3);
            switch (state) {
                case 0:
                    return super.getGraphTraversalSource(graphData).withComputer();
                case 1:
                    return super.getGraphTraversalSource(graphData).withComputer(Computer.compute(TinkerGraphComputer.class));
                case 2:
                    return super.getGraphTraversalSource(graphData)
                            .withComputer(Computer.compute(TinkerGraphComputer.class)
                                    .workers(TestHelper.RANDOM.nextInt(AVAILABLE_PROCESSORS) + 1));
                default:
                    throw new IllegalStateException("This state should not have occurred: " + state);
            }
        }

        @Override
        public void beforeEachScenario(Scenario scenario) {
            super.beforeEachScenario(scenario);

            switch (scenario.getName()) {
                // TINKERPOP-3208
                case "g_V_repeatXunionXoutXknowsX_order_byXnameX_inXcreatedX_order_byXnameXXX_timesX1X":
                case "g_V_repeatXboth_repeatXorder_byXnameXX_timesX1XX_timesX1X":
                case "g_V_order_byXname_descX_repeatXboth_simplePath_order_byXname_descXX_timesX2X_path":
                case "g_VX3X_repeatXoutE_order_byXweightX_tailX2X_inVX_timesX2X_valuesXnameX":
                case "g_VX3X_repeatXout_order_byXperformances_descX_limitX5X_tailX1XX_timesX2X_valuesXnameX":
                case "g_VX3X_repeatXout_order_byXperformancesX_tailX3X_limitX1XX_timesX2X_valuesXnameX":
                case "g_VX250X_repeatXout_localXorder_byXperformancesX_tailX1XXX_timesX2X_valuesXnameX":
                case "g_VX3X_repeatXout_order_byXperformancesX_tailX2XX_timesX2X_valuesXnameX":
                    throw new AssumptionViolatedException("GraphComputer doesn't order within repeat");
                // TINKERPOP-3209
                case "g_V_localXgroupCountXaX_selectXaX_countXlocalXX":
                case "g_V_localXgroupXaX_byXnameX_by_selectXaX_countXlocalXX":
                case "g_V_out_order_byXnameX_localXtreeXaX_selectXaX_countXlocalXX":
                    throw new AssumptionViolatedException("Selecting side effects inside local() produces inconsistent results in OLAP (See TINKERPOP-3209)");
                default:
                    // Do nothing
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class GraphBinaryRemoteWorld extends RemoteWorld {
        public GraphBinaryRemoteWorld() { super(createTestCluster(Serializers.GRAPHBINARY_V1)); }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class GraphBinaryRemoteComputerWorld extends RemoteComputerWorld {
        public GraphBinaryRemoteComputerWorld() { super(createTestCluster(Serializers.GRAPHBINARY_V1)); }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class GraphSONRemoteWorld extends RemoteWorld {
        public GraphSONRemoteWorld() { super(createTestCluster(Serializers.GRAPHSON_V3)); }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static class GraphSONRemoteComputerWorld extends RemoteComputerWorld {
        public GraphSONRemoteComputerWorld() { super(createTestCluster(Serializers.GRAPHSON_V3)); }
    }
}
