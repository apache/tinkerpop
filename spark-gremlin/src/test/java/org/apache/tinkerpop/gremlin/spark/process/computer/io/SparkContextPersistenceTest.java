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

package org.apache.tinkerpop.gremlin.spark.process.computer.io;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.process.computer.util.SparkHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoIo;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkContextPersistenceTest {

    @Test
    public void shouldPersistRDDAcrossJobs() throws Exception {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", "org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer");
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, NullOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD_NAME, "a-random-name-for-testing");
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.NOTHING)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ////////
        SparkConf sparkConfiguration = new SparkConf();
        sparkConfiguration.setAppName("shouldPersistRDDAcrossJobs");
        ConfUtil.makeHadoopConfiguration(configuration).forEach(entry -> sparkConfiguration.set(entry.getKey(), entry.getValue()));
        JavaSparkContext sparkContext = new JavaSparkContext(SparkContext.getOrCreate(sparkConfiguration));
        assertTrue(SparkHelper.getPersistedRDD(sparkContext, "a-random-name-for-testing").isPresent());
        ///////
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD_NAME, "a-random-name-for-testing");
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD_NAME, null);
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.NOTHING)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
    }

    @Test
    public void testBulkLoaderVertexProgramChain() throws Exception {
        final Configuration readConfiguration = new BaseConfiguration();
        readConfiguration.setProperty("spark.master", "local[4]");
        readConfiguration.setProperty("spark.serializer", "org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer");
        readConfiguration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, NullOutputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "target/test-output");
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD_NAME, "a-random-name-for-testing");
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph graph = GraphFactory.open(readConfiguration);

        ///////////////
        final Configuration writeConfiguration = new BaseConfiguration();
        writeConfiguration.setProperty(Graph.GRAPH, "org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph");
        writeConfiguration.setProperty(TinkerGraph.CONFIG_GRAPH_FORMAT, "gryo");
        writeConfiguration.setProperty(TinkerGraph.CONFIG_GRAPH_LOCATION, "target/test-output/tinkergraph.kryo");
        final Graph secondGraph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.NOTHING).program(PageRankVertexProgram.build().create(graph)).submit().get().graph();
        secondGraph.configuration().setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD_NAME, "a-random-name-for-testing");
        secondGraph.configuration().setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD_NAME, null);
        secondGraph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.NOTHING)
                .workers(1)
                .program(BulkLoaderVertexProgram.build().userSuppliedIds(true).writeGraph(writeConfiguration).create(secondGraph))
                .submit().get();
        final Graph finalGraph = TinkerGraph.open();
        finalGraph.io(IoCore.gryo()).readGraph("target/test-output/tinkergraph.kryo");
        assertEquals(6l,finalGraph.traversal().V().count().next().longValue());
    }
}
