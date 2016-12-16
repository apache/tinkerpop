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

package org.apache.tinkerpop.gremlin.spark.structure.io.partitioner;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.partitioner.PartitionerInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerEdge;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerElement;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraphVariables;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerVertexProperty;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = SparkGraphComputer.class)
public class TinkerGraphPartitionerProvider extends AbstractGraphProvider {

    private static Set<String> SKIP_TESTS = new HashSet<>(Arrays.asList(
            TraversalInterruptionComputerTest.class.getCanonicalName(),
            PageRankTest.Traversals.class.getCanonicalName(),
            ProgramTest.Traversals.class.getCanonicalName(),
            PeerPressureTest.Traversals.class.getCanonicalName(),
            BulkLoaderVertexProgramTest.class.getCanonicalName(),
            PageRankVertexProgramTest.class.getCanonicalName(),
            ReadOnlyStrategyProcessTest.class.getCanonicalName(),
            "testProfileStrategyCallback",
            "testProfileStrategyCallbackSideEffect",
            "shouldSucceedWithProperTraverserRequirements",
            "shouldStartAndEndWorkersForVertexProgramAndMapReduce",
            "shouldFailWithImproperTraverserRequirements"));

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(TinkerEdge.class);
        add(TinkerElement.class);
        add(TinkerGraph.class);
        add(TinkerGraphVariables.class);
        add(TinkerProperty.class);
        add(TinkerVertex.class);
        add(TinkerVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {

        final TinkerGraph.DefaultIdManager idManager = selectIdMakerFromGraphData(loadGraphWith);
        final String idMaker = (idManager.equals(TinkerGraph.DefaultIdManager.ANY) ? selectIdMakerFromGraphData(loadGraphWith) : idManager).name();
        final boolean skipTest = SKIP_TESTS.contains(testMethodName) || SKIP_TESTS.contains(test.getCanonicalName());
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, TinkerGraph.class.getName());
            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, idMaker);
            put(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, idMaker);
            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, idMaker);
            put("skipTest", skipTest);
            if (loadGraphWith == LoadGraphWith.GraphData.CREW)
                put(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
            put("spark.master", "local[4]");
            put("spark.serializer", GryoSerializer.class.getCanonicalName());
            put("spark.kryo.registrationRequired", true);
            put(Constants.GREMLIN_HADOOP_GRAPH_READER, PartitionerInputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
            put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, getWorkingDirectory());
            put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
            if (!skipTest)
                put(GraphComputer.GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
            put(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
            put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
            // System.out.println(AbstractGremlinTest.class.getResource(loadGraphWith.location()).toString().replace("file:",""));
            if (null != loadGraphWith)
                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, AbstractGremlinTest.class.getResource(loadGraphWith.location()).toString().replace("file:", ""));
        }};


    }

    protected void readIntoGraph(final Graph g, final String path) throws IOException {

    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        if (null != graph)
            ((TinkerGraph) graph).clear();
        // if (graph != null)
        //    graph.close();
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
        if (graph.configuration().getBoolean("skipTest", false))
            return graph.traversal().withStrategies(ReadOnlyStrategy.instance()).withComputer();
        else
            return graph.traversal().withStrategies(ReadOnlyStrategy.instance()).withProcessor(SparkGraphComputer.open(graph.configuration()));
    }

    @Override
    public GraphComputer getGraphComputer(final Graph graph) {
        if (graph.configuration().getBoolean("skipTest", false))
            return new TinkerGraphComputer((TinkerGraph) graph);
        else
            return SparkGraphComputer.open(graph.configuration());
    }
}