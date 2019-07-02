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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedInputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoSerializerIntegrateTest extends AbstractSparkTest {

    @Test
    public void shouldHaveAllRegisteredGryoSerializerClasses() throws Exception {
        // this is a stress test that ensures that when data is spilling to disk, persisted to an RDD, etc. the correct classes are registered with GryoSerializer.
        final TinkerGraph randomGraph = TinkerGraph.open();
        int totalVertices = 200000;
        TestHelper.createRandomGraph(randomGraph, totalVertices, 100);
        final String inputLocation = TestHelper.makeTestDataDirectory(GryoSerializerIntegrateTest.class, UUID.randomUUID().toString()) + "/random-graph.kryo";
        randomGraph.io(IoCore.gryo()).writeGraph(inputLocation);
        randomGraph.clear();
        randomGraph.close();

        final String outputLocation = TestHelper.makeTestDataDirectory(GryoSerializerIntegrateTest.class, UUID.randomUUID().toString());
        Configuration configuration = getBaseConfiguration();
        configuration.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, false);
        Graph graph = GraphFactory.open(configuration);
        final GraphTraversal.Admin<Vertex, Map<Vertex, Collection<Vertex>>> traversal = graph.traversal().withComputer(SparkGraphComputer.class).V().group("m").<Map<Vertex, Collection<Vertex>>>cap("m").asAdmin();
        assertTrue(traversal.hasNext());
        assertEquals(traversal.next(), traversal.getSideEffects().get("m"));
        assertFalse(traversal.hasNext());
        assertTrue(traversal.getSideEffects().exists("m"));
        assertTrue(traversal.getSideEffects().get("m") instanceof Map);
        assertEquals(totalVertices, traversal.getSideEffects().<Map>get("m").size());

        configuration = getBaseConfiguration();
        configuration.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, inputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "DISK_ONLY");
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, "persisted-rdd");
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        graph = GraphFactory.open(configuration);
        assertEquals(totalVertices, graph.compute(SparkGraphComputer.class).program(PageRankVertexProgram.build().iterations(2).create(graph)).submit().get().graph().traversal().V().count().next().longValue());

        configuration = getBaseConfiguration();
        configuration.clearProperty(Constants.SPARK_SERIALIZER); // ensure proper default to GryoSerializer
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "persisted-rdd");
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        graph = GraphFactory.open(configuration);
        assertEquals(totalVertices, graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue());

        configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, "persisted-rdd");
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, PersistedInputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_STORAGE_LEVEL, "MEMORY_ONLY"); // this should be ignored as you can't change the persistence level once created
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_AND_DISK");
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        graph = GraphFactory.open(configuration);
        assertEquals(totalVertices, graph.traversal().withComputer(SparkGraphComputer.class).V().count().next().longValue());
    }
}
