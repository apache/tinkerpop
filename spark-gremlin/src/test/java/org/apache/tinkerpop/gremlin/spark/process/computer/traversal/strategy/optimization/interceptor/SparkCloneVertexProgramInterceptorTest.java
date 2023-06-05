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
package org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor;

import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.clone.CloneVertexProgram;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializerIntegrateTest;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;

/**
 * This class verify the default exporting behavior which requires the CloneVertexProgram's execute() is empty,
 * then the SparkCloneVertexProgramInterceptor will bypass the VertexProgram.
 */
public class SparkCloneVertexProgramInterceptorTest extends AbstractSparkTest {
    @Test
    public void shouldExportCorrectGraph() throws Exception {
        // Build the random graph
        final TinkerGraph randomGraph = TinkerGraph.open();
        final int totalVertices = 200000;
        TestHelper.createRandomGraph(randomGraph, totalVertices, 100);
        final String inputLocation = TestHelper.makeTestDataFile(GryoSerializerIntegrateTest.class,
                UUID.randomUUID().toString(),
                "random-graph.kryo");
        randomGraph.io(IoCore.gryo()).writeGraph(inputLocation);
        randomGraph.clear();
        randomGraph.close();

        // Serialize the graph to disk by CloneVertexProgram + SparkGraphComputer
        final String outputLocation = TestHelper.makeTestDataDirectory(GryoSerializerIntegrateTest.class, UUID.randomUUID().toString());
        Configuration configuration = getBaseConfiguration();
        configuration.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        Graph graph = GraphFactory.open(configuration);
        graph.compute(SparkGraphComputer.class).program(CloneVertexProgram.build().create()).submit().get();

        // Read the total Vertex/Edge count for golden reference through the original graph
        configuration = getBaseConfiguration();
        configuration.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, NullOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        graph = GraphFactory.open(configuration);
        long totalVRef = graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue();
        long totalERef = graph.traversal().withComputer(SparkGraphComputer.class).E().count().next().longValue();

        assertEquals(totalVRef, totalVertices);
        // Read the total Vertex/Edge count from the exported graph
        configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, NullOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_ONLY"); // this should be ignored as you can't change the persistence level once created
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_AND_DISK");
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        graph = GraphFactory.open(configuration);
        long totalV = graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue();
        long totalE = graph.traversal().withComputer(SparkGraphComputer.class).E().count().next().longValue();
        assertEquals(totalV, totalVRef);
        assertEquals(totalE, totalERef);
    }

    @Test
    public void shouldStopPurgingOfExistingNonEmptyFolder() throws Exception {
        // Build the random graph
        final TinkerGraph randomGraph = TinkerGraph.open();
        final int totalVertices = 200000;
        TestHelper.createRandomGraph(randomGraph, totalVertices, 100);
        final String inputLocation = TestHelper.makeTestDataFile(GryoSerializerIntegrateTest.class,
                UUID.randomUUID().toString(),
                "random-graph.kryo");
        randomGraph.io(IoCore.gryo()).writeGraph(inputLocation);
        randomGraph.clear();
        randomGraph.close();

        // Serialize the graph to disk by CloneVertexProgram + SparkGraphComputer
        final String outputLocation = TestHelper.makeTestDataDirectory(GryoSerializerIntegrateTest.class, UUID.randomUUID().toString());
        Configuration configuration1 = getBaseConfiguration();
        configuration1.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration1.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration1.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration1.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration1.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration1.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        Graph graph = GraphFactory.open(configuration1);
        graph.compute(SparkGraphComputer.class).program(CloneVertexProgram.build().create()).submit().get();

        // Read the total Vertex/Edge count for golden reference through the original graph
        Configuration configuration2 = getBaseConfiguration();
        configuration2.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration2.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration2.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration2.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, NullOutputFormat.class.getCanonicalName());
        configuration2.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        configuration2.setProperty(Constants.GREMLIN_SPARK_DONT_DELETE_NON_EMPTY_OUTPUT, true);
        graph = GraphFactory.open(configuration2);
        long totalVRef = graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue();

        assertEquals(totalVRef, totalVertices);
        // Should see exception if reuse the previous outputLocation which is not empty
        graph = GraphFactory.open(configuration1);
        try {
            graph.traversal().withComputer(SparkGraphComputer.class).E().count().next().longValue();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("is not empty"));
        }
    }
}
