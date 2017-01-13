/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.akka.process.actors;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.akka.process.actors.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.process.actors.GraphActors;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComplexTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProfileTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONResourceAccess;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoResourceAccess;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphVariables;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AkkaActorsProvider extends AbstractGraphProvider {

    private static final Random RANDOM = new Random();

    private static Set<String> SKIP_TESTS = new HashSet<>(Arrays.asList(
            "g_V_hasLabelXpersonX_V_hasLabelXsoftwareX_name",
            "g_VX1X_repeatXbothEXcreatedX_whereXwithoutXeXX_aggregateXeX_otherVX_emit_path",
            "g_withBulkXfalseX_withSackX1_sumX_V_out_barrier_sack",
            "g_V_both_groupCountXaX_out_capXaX_selectXkeysX_unfold_both_groupCountXaX_capXaX",
            GraphTest.Traversals.class.getCanonicalName(),
            GroupTest.Traversals.class.getCanonicalName(),
            ComplexTest.Traversals.class.getCanonicalName(),
            SubgraphTest.Traversals.class.getCanonicalName(),
            SideEffectTest.Traversals.class.getCanonicalName(),
            SubgraphStrategyProcessTest.class.getCanonicalName(),
            ProfileTest.Traversals.class.getCanonicalName(),
            PartitionStrategyProcessTest.class.getCanonicalName(),
            EventStrategyProcessTest.class.getCanonicalName(),
            ElementIdStrategyProcessTest.class.getCanonicalName(),
            TraversalInterruptionTest.class.getCanonicalName()));

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(TinkerEdge.class);
        add(TinkerElement.class);
        add(TinkerGraph.class);
        add(TinkerGraphVariables.class);
        add(TinkerProperty.class);
        add(TinkerVertex.class);
        add(TinkerVertexProperty.class);
    }};

    private static Map<String, String> PATHS = new HashMap<>();

    static {
        try {
            final List<String> kryoResources = Arrays.asList(
                    "tinkerpop-modern.kryo",
                    "grateful-dead.kryo",
                    "tinkerpop-classic.kryo",
                    "tinkerpop-crew.kryo");
            for (final String fileName : kryoResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GryoResourceAccess.class, fileName, "").getAbsolutePath().replace('\\', '/'));
            }

            final List<String> graphsonResources = Arrays.asList(
                    "tinkerpop-modern-v2d0-typed.json",
                    "grateful-dead-v2d0-typed.json",
                    "tinkerpop-classic-v2d0-typed.json",
                    "tinkerpop-crew-v2d0-typed.json");
            for (final String fileName : graphsonResources) {
                PATHS.put(fileName, TestHelper.generateTempFileFromResource(GraphSONResourceAccess.class, fileName, "").getAbsolutePath().replace('\\', '/'));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {

        final TinkerGraph.DefaultIdManager idManager = selectIdMakerFromGraphData(loadGraphWith);
        final String idMaker = (idManager.equals(TinkerGraph.DefaultIdManager.ANY) ? selectIdMakerFromGraphData(loadGraphWith) : idManager).name();
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, TinkerGraph.class.getName());
            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, idMaker);
            put(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, idMaker);
            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, idMaker);
            put("skipTest", SKIP_TESTS.contains(testMethodName) || SKIP_TESTS.contains(test.getCanonicalName()));
            if (null != loadGraphWith) {
                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, loadGraphDataViaHadoopConfig(loadGraphWith));
                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
                if (loadGraphWith == LoadGraphWith.GraphData.CREW)
                    put(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
            }
            // Akka specific configuration
            put(Constants.AKKA_LOG_DEAD_LETTERS_DURING_SHUTDOWN, false);
            put(Constants.AKKA_ACTOR_PROVIDER, "remote");
            put(Constants.AKKA_ACTOR_SERIALIZE_MESSAGES, "on");
            put(Constants.AKKA_ACTOR_SERIALIZERS_GRYO, GryoSerializer.class.getCanonicalName());
            put(Constants.AKKA_REMOTE_ENABLED_TRANSPORTS, Collections.singletonList("akka.remote.netty.tcp"));
            put(Constants.AKKA_REMOTE_NETTY_TCP_HOSTNAME, "127.0.0.1");
            put(Constants.AKKA_REMOTE_NETTY_TCP_PORT, 2552);
        }};
    }

    public String loadGraphDataViaHadoopConfig(final LoadGraphWith.GraphData graphData) {
        final String type = ".kryo";
        if (graphData.equals(LoadGraphWith.GraphData.GRATEFUL)) {
            return PATHS.get("grateful-dead" + type);
        } else if (graphData.equals(LoadGraphWith.GraphData.MODERN)) {
            return PATHS.get("tinkerpop-modern" + type);
        } else if (graphData.equals(LoadGraphWith.GraphData.CLASSIC)) {
            return PATHS.get("tinkerpop-classic" + type);
        } else if (graphData.equals(LoadGraphWith.GraphData.CREW)) {
            return PATHS.get("tinkerpop-crew" + type);
        } else {
            throw new RuntimeException("Could not load graph with " + graphData);
        }
    }

    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {

    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        //  if (graph != null) graph.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    /**
     * Test that load with specific graph data can be configured with a specific id manager as the data type to
     * be used in the test for that graph is known.
     */
    protected TinkerGraph.DefaultIdManager selectIdMakerFromGraphData(final LoadGraphWith.GraphData loadGraphWith) {
        if (null == loadGraphWith) return TinkerGraph.DefaultIdManager.ANY;
        if (loadGraphWith.equals(LoadGraphWith.GraphData.CLASSIC))
            return TinkerGraph.DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
            return TinkerGraph.DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.CREW))
            return TinkerGraph.DefaultIdManager.INTEGER;
        else if (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
            return TinkerGraph.DefaultIdManager.INTEGER;
        else
            throw new IllegalStateException(String.format("Need to define a new %s for %s", TinkerGraph.IdManager.class.getName(), loadGraphWith.name()));
    }

/////////////////////////////
/////////////////////////////
/////////////////////////////

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if ((Boolean) graph.configuration().getProperty("skipTest"))
            return graph.traversal();
            //throw new VerificationException("This test current does not work with Gremlin-Python", EmptyTraversal.instance());
        else {
            final GraphTraversalSource g = graph.traversal();
            return RANDOM.nextBoolean() ?
                    g.withProcessor(AkkaGraphActors.open().workers(new Random().nextInt(15) + 1)) :
                    g.withProcessor(GraphActors.open(AkkaGraphActors.class));
        }
    }

    @Override
    public GraphActors getGraphActors(final Graph graph) {
        return AkkaGraphActors.open().workers(new Random().nextInt(15) + 1);
    }
}