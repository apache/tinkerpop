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

package org.apache.tinkerpop.gremlin.spark.structure.io;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.ComputerTraversalEngine;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PersistedInputOutputRDDTest extends AbstractSparkTest {

    @Test
    public void shouldNotPersistRDDAcrossJobs() throws Exception {
        Spark.create("local[4]");
        final String rddLocation = "target/test-output/" + UUID.randomUUID();
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);  // because the spark context is NOT persisted, neither is the RDD
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ////////
        Spark.create("local[4]");
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        Spark.close();
    }

    @Test
    public void shouldPersistRDDAcrossJobs() throws Exception {
        final String rddLocation = "target/test-output/" + UUID.randomUUID();
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ////////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ///////
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null);
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null);
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.NOTHING)
                .program(TraversalVertexProgram.build()
                        .traversal(GraphTraversalSource.build().engine(ComputerTraversalEngine.build().computer(SparkGraphComputer.class)),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        Spark.close();
    }

    @Test
    public void testBulkLoaderVertexProgramChain() throws Exception {
        Spark.create("local[4]");

        final String rddLocation = "target/test-output/" + UUID.randomUUID().toString();
        final Configuration readConfiguration = new BaseConfiguration();
        readConfiguration.setProperty("spark.master", "local[4]");
        readConfiguration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        readConfiguration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph pageRankGraph = GraphFactory.open(readConfiguration);
        ///////////////
        final Configuration writeConfiguration = new BaseConfiguration();
        writeConfiguration.setProperty(Graph.GRAPH, TinkerGraph.class.getCanonicalName());
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, "target/test-output/testBulkLoaderVertexProgramChain.kryo");
        final Graph bulkLoaderGraph = pageRankGraph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.VERTEX_PROPERTIES).program(PageRankVertexProgram.build().create(pageRankGraph)).submit().get().graph();
        bulkLoaderGraph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.NOTHING)
                .workers(1)
                .configure(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, PersistedInputRDD.class.getCanonicalName())
                .configure(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddLocation)
                .configure(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, null)
                .configure(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null)
                .program(BulkLoaderVertexProgram.build().userSuppliedIds(true).writeGraph(writeConfiguration).create(bulkLoaderGraph))
                .submit().get();
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ////
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        graph.io(IoCore.gryo()).readGraph("target/test-output/testBulkLoaderVertexProgramChain.kryo");
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals("marko", g.V().has("name", "marko").values("name").next());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        Spark.close();
    }

    @Test
    public void testBulkLoaderVertexProgramChainWithInputOutputHelperMapping() throws Exception {
        Spark.create("local[4]");

        final String rddLocation = "target/test-output/" + UUID.randomUUID().toString();
        final Configuration readConfiguration = new BaseConfiguration();
        readConfiguration.setProperty("spark.master", "local[4]");
        readConfiguration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        readConfiguration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph pageRankGraph = GraphFactory.open(readConfiguration);
        ///////////////
        final Configuration writeConfiguration = new BaseConfiguration();
        writeConfiguration.setProperty(Graph.GRAPH, TinkerGraph.class.getCanonicalName());
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, "target/test-output/testBulkLoaderVertexProgramChainWithInputOutputHelperMapping.kryo");
        final Graph bulkLoaderGraph = pageRankGraph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).program(PageRankVertexProgram.build().create(pageRankGraph)).submit().get().graph();
        bulkLoaderGraph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.NOTHING)
                .workers(1)
                .program(BulkLoaderVertexProgram.build().userSuppliedIds(true).writeGraph(writeConfiguration).create(bulkLoaderGraph))
                .submit().get();
        ////
        Spark.create(readConfiguration);
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ////
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        graph.io(IoCore.gryo()).readGraph("target/test-output/testBulkLoaderVertexProgramChainWithInputOutputHelperMapping.kryo");
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals("marko", g.V().has("name", "marko").values("name").next());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        Spark.close();
    }

    @Test
    public void testComplexChain() throws Exception {
        Spark.create("local[4]");

        final String rddLocation = "target/test-output/" + UUID.randomUUID();
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_INPUT_FORMAT, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);

        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        Graph graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        GraphTraversalSource g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ////
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_INPUT_RDD, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddLocation);
        ////
        graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ////
        graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.VERTEX_PROPERTIES).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        ////
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.NOTHING).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        g = graph.traversal();
        assertEquals(0l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals(0l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        assertEquals(0l, g.V().values(PageRankVertexProgram.EDGE_COUNT).count().next().longValue());
        ////
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddLocation)));
        Spark.close();
    }
}
