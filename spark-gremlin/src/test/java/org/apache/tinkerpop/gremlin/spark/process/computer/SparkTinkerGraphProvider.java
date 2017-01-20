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

package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.commons.configuration.Configuration;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PageRankTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PeerPressureTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProgramTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.AbstractTinkerGraphProvider;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputer;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool.CONFIG_IO_GRYO_POOL_SIZE;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@GraphProvider.Descriptor(computer = SparkGraphComputer.class)
public class SparkTinkerGraphProvider extends AbstractTinkerGraphProvider {

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

    @Override
    public Set<String> getSkipTests() {
        return SKIP_TESTS;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
                                                    final LoadGraphWith.GraphData loadGraphWith) {

        final Map<String, Object> configuration = super.getBaseConfiguration(graphName, test, testMethodName, loadGraphWith);

        // Spark specific configuration
        configuration.put("spark.master", "local[4]");
        configuration.put("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.put("spark.kryo.registrationRequired", true);
        configuration.put(Constants.GREMLIN_HADOOP_GRAPH_READER, PartitionerInputFormat.class.getCanonicalName());
        configuration.put(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.put(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, getWorkingDirectory());
        configuration.put(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.put(CONFIG_IO_GRYO_POOL_SIZE, 16);
        if (!(Boolean) configuration.getOrDefault(GREMLIN_TINKERGRAPH_SKIP_TEST, false))
            configuration.put(GraphComputer.GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        configuration.put(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        return configuration;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) throws Exception {
        //  don't clear loaded data files
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        if (graph.configuration().getBoolean(GREMLIN_TINKERGRAPH_SKIP_TEST, false))
            return graph.traversal().withStrategies(ReadOnlyStrategy.instance()).withComputer();
        else
            return graph.traversal().withStrategies(ReadOnlyStrategy.instance()).withProcessor(SparkGraphComputer.open(graph.configuration()));
    }

    @Override
    public GraphComputer getGraphComputer(final Graph graph) {
        if (graph.configuration().getBoolean(GREMLIN_TINKERGRAPH_SKIP_TEST, false))
            return new TinkerGraphComputer((TinkerGraph) graph);
        else
            return SparkGraphComputer.open(graph.configuration());
    }
}