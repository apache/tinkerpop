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
import org.apache.spark.storage.StorageLevel;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.Spark;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PersistedInputOutputRDDIntegrateTest extends AbstractSparkTest {

    @Test
    public void shouldNotHaveDanglingPersistedComputeRDDs() throws Exception {
        Spark.create("local[4]");
        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration configuration = super.getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph graph = GraphFactory.open(configuration);
        ///
        assertEquals(6, graph.traversal().withComputer(Computer.compute(SparkGraphComputer.class)).V().out().count().next().longValue());
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(0, Spark.getContext().getPersistentRDDs().size());
        //
        assertEquals(2, graph.traversal().withComputer(Computer.compute(SparkGraphComputer.class)).V().out().out().count().next().longValue());
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(0, Spark.getContext().getPersistentRDDs().size());
        ///////
        Spark.close();
    }

    @Test
    public void shouldPersistRDDBasedOnStorageLevel() throws Exception {
        Spark.create("local[4]");
        int counter = 0;
        for (final String storageLevel : Arrays.asList("MEMORY_ONLY", "DISK_ONLY", "MEMORY_ONLY_SER", "MEMORY_AND_DISK_SER")) {
            assertEquals(counter, Spark.getRDDs().size());
            assertEquals(counter, Spark.getContext().getPersistentRDDs().size());
            counter++;
            final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
            final Configuration configuration = super.getBaseConfiguration();
            configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
            configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
            configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
            configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, storageLevel);
            configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
            configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
            Graph graph = GraphFactory.open(configuration);
            graph.compute(SparkGraphComputer.class)
                    .result(GraphComputer.ResultGraph.NEW)
                    .persist(GraphComputer.Persist.EDGES)
                    .program(TraversalVertexProgram.build()
                            .traversal(graph.traversal().withComputer(SparkGraphComputer.class),
                                    "gremlin-groovy",
                                    "g.V().groupCount('m').by('name').out()").create(graph)).submit().get();
            ////////
            assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
            assertEquals(StorageLevel.fromString(storageLevel), Spark.getRDD(Constants.getGraphLocation(rddName)).getStorageLevel());
            assertEquals(counter, Spark.getRDDs().size());
            assertEquals(counter, Spark.getContext().getPersistentRDDs().size());
        }
        Spark.close();
    }

    @Test
    public void shouldNotPersistRDDAcrossJobs() throws Exception {
        Spark.create("local[4]");
        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration configuration = super.getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);  // because the spark context is NOT persisted, neither is the RDD
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(SparkGraphComputer.class),
                                "gremlin-groovy",
                                "g.V()").create(graph)).submit().get();
        ////////
        Spark.create("local[4]");
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(0, Spark.getContext().getPersistentRDDs().size());
        Spark.close();
    }

    @Test
    public void shouldPersistRDDAcrossJobs() throws Exception {
        Spark.create("local[4]");
        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final String rddName2 = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration configuration = super.getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(SparkGraphComputer.class),
                                "gremlin-groovy",
                                "g.V().count()").create(graph)).submit().get();
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ///////
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName2);
        graph = GraphFactory.open(configuration);
        assertEquals(6, graph.traversal().withComputer(SparkGraphComputer.class).V().out().count().next().longValue());
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ///////
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(SparkGraphComputer.class),
                                "gremlin-groovy",
                                "g.V().count()").create(graph)).submit().get();
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        assertEquals(2, Spark.getContext().getPersistentRDDs().size());
        ///////
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName2);
        graph = GraphFactory.open(configuration);
        assertEquals(6, graph.traversal().withComputer(SparkGraphComputer.class).V().out().count().next().longValue());
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ///////
        graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class)
                .result(GraphComputer.ResultGraph.NEW)
                .persist(GraphComputer.Persist.EDGES)
                .program(TraversalVertexProgram.build()
                        .traversal(graph.traversal().withComputer(SparkGraphComputer.class),
                                "gremlin-groovy",
                                "g.V().count()").create(graph)).submit().get();
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        assertEquals(2, Spark.getContext().getPersistentRDDs().size());
        ///////
        graph = GraphFactory.open(configuration);
        assertEquals(6, graph.traversal().withComputer(SparkGraphComputer.class).V().out().count().next().longValue());
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        Spark.close();
    }

    @Test
    public void testBulkLoaderVertexProgramChain() throws Exception {
        Spark.create("local[4]");
        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration readConfiguration = super.getBaseConfiguration();
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph pageRankGraph = GraphFactory.open(readConfiguration);
        ///////////////
        final Configuration writeConfiguration = new BaseConfiguration();
        writeConfiguration.setProperty(Graph.GRAPH, TinkerGraph.class.getCanonicalName());
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, TestHelper.makeTestDataFile(PersistedInputOutputRDDIntegrateTest.class, "testBulkLoaderVertexProgramChain.kryo"));
        final Graph bulkLoaderGraph = pageRankGraph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.VERTEX_PROPERTIES).program(PageRankVertexProgram.build().create(pageRankGraph)).submit().get().graph();
        bulkLoaderGraph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.NOTHING)
                .workers(1)
                .configure(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName())
                .configure(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddName)
                .configure(Constants.GREMLIN_HADOOP_GRAPH_WRITER, null)
                .configure(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, null)
                .program(BulkLoaderVertexProgram.build().userSuppliedIds(true).writeGraph(writeConfiguration).create(bulkLoaderGraph))
                .submit().get();
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ////
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        graph.io(IoCore.gryo()).readGraph(TestHelper.makeTestDataFile(PersistedInputOutputRDDIntegrateTest.class, "testBulkLoaderVertexProgramChain.kryo"));
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals("marko", g.V().has("name", "marko").values("name").next());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        Spark.close();
    }

    @Test
    public void testBulkLoaderVertexProgramChainWithInputOutputHelperMapping() throws Exception {
        Spark.create("local[4]");

        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, UUID.randomUUID().toString());
        final Configuration readConfiguration = super.getBaseConfiguration();
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        readConfiguration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        readConfiguration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        Graph pageRankGraph = GraphFactory.open(readConfiguration);
        ///////////////
        final Configuration writeConfiguration = new BaseConfiguration();
        writeConfiguration.setProperty(Graph.GRAPH, TinkerGraph.class.getCanonicalName());
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
        writeConfiguration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, TestHelper.makeTestDataFile(PersistedInputOutputRDDIntegrateTest.class, "testBulkLoaderVertexProgramChainWithInputOutputHelperMapping.kryo"));
        final Graph bulkLoaderGraph = pageRankGraph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).program(PageRankVertexProgram.build().create(pageRankGraph)).submit().get().graph();
        bulkLoaderGraph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.NOTHING)
                .workers(1)
                .program(BulkLoaderVertexProgram.build().userSuppliedIds(true).writeGraph(writeConfiguration).create(bulkLoaderGraph))
                .submit().get();
        ////
        Spark.create(readConfiguration);
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ////
        final Graph graph = TinkerGraph.open();
        final GraphTraversalSource g = graph.traversal();
        graph.io(IoCore.gryo()).readGraph(TestHelper.makeTestDataFile(PersistedInputOutputRDDIntegrateTest.class, "testBulkLoaderVertexProgramChainWithInputOutputHelperMapping.kryo"));
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals("marko", g.V().has("name", "marko").values("name").next());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        Spark.close();
    }

    @Test
    public void testComplexChain() throws Exception {
        Spark.create("local[4]");

        final String rddName = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, "testComplexChain", "graphRDD");
        final String rddName2 = TestHelper.makeTestDataDirectory(PersistedInputOutputRDDIntegrateTest.class, "testComplexChain", "graphRDD2");
        final Configuration configuration = super.getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);

        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(0, Spark.getContext().getPersistentRDDs().size());
        Graph graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        GraphTraversalSource g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        ////
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, rddName);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, rddName2);
        ////
        graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.EDGES).mapReduce(PageRankMapReduce.build().create()).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(6l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        assertTrue(Spark.hasRDD(Constants.getMemoryLocation(rddName2, PageRankMapReduce.DEFAULT_MEMORY_KEY)));
        assertEquals(3, Spark.getContext().getPersistentRDDs().size());
        ////
        graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.VERTEX_PROPERTIES).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        g = graph.traversal();
        assertEquals(6l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals(6l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        assertFalse(Spark.hasRDD(Constants.getMemoryLocation(rddName2, PageRankMapReduce.DEFAULT_MEMORY_KEY)));
        assertEquals(2, Spark.getContext().getPersistentRDDs().size());
        ////
        graph = GraphFactory.open(configuration);
        graph = graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.NOTHING).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph();
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        g = graph.traversal();
        assertEquals(0l, g.V().count().next().longValue());
        assertEquals(0l, g.E().count().next().longValue());
        assertEquals(0l, g.V().values(PageRankVertexProgram.PAGE_RANK).count().next().longValue());
        ////
        assertTrue(Spark.hasRDD(Constants.getGraphLocation(rddName)));
        assertFalse(Spark.hasRDD(Constants.getGraphLocation(rddName2)));
        assertFalse(Spark.hasRDD(Constants.getMemoryLocation(rddName2, PageRankMapReduce.DEFAULT_MEMORY_KEY)));
        assertEquals(1, Spark.getContext().getPersistentRDDs().size());
        Spark.close();
    }
}
