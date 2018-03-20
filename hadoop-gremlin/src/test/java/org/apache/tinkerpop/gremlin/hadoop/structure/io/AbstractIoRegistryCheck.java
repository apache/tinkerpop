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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.ToyIoRegistry;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.ToyPoint;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.ToyTriangle;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractIoRegistryCheck extends AbstractGremlinTest {

    private static final int NUMBER_OF_VERTICES = 1000;

    public void checkGryoV1d0IoRegistryCompliance(final HadoopGraph graph, final Class<? extends GraphComputer> graphComputerClass) throws Exception {
        final File input = TestHelper.generateTempFile(this.getClass(), "gryo-io-registry", ".kryo");
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        graph.configuration().setProperty(GryoPool.CONFIG_IO_GRYO_VERSION, GryoVersion.V1_0.name());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input.getAbsolutePath());
        graph.configuration().setProperty(IoRegistry.IO_REGISTRY, ToyIoRegistry.class.getCanonicalName());
        final GryoRecordWriter writer = new GryoRecordWriter(new DataOutputStream(new FileOutputStream(input)), ConfUtil.makeHadoopConfiguration(graph.configuration()));
        validateIoRegistryGraph(graph, graphComputerClass, writer);
        assertTrue(input.delete());
    }

    public void checkGryoV3d0IoRegistryCompliance(final HadoopGraph graph, final Class<? extends GraphComputer> graphComputerClass) throws Exception {
        final File input = TestHelper.generateTempFile(this.getClass(), "gryo-io-registry", ".kryo");
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input.getAbsolutePath());
        graph.configuration().setProperty(GryoPool.CONFIG_IO_GRYO_VERSION, GryoVersion.V3_0.name());
        graph.configuration().setProperty(IoRegistry.IO_REGISTRY, ToyIoRegistry.class.getCanonicalName());
        final GryoRecordWriter writer = new GryoRecordWriter(new DataOutputStream(new FileOutputStream(input)), ConfUtil.makeHadoopConfiguration(graph.configuration()));
        validateIoRegistryGraph(graph, graphComputerClass, writer);
        assertTrue(input.delete());
    }

    public void checkGraphSONIoRegistryCompliance(final HadoopGraph graph, final Class<? extends GraphComputer> graphComputerClass) throws Exception {
        final File input = TestHelper.generateTempFile(this.getClass(), "graphson-io-registry", ".json");
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GraphSONInputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GraphSONOutputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input.getAbsolutePath());
        graph.configuration().setProperty(IoRegistry.IO_REGISTRY, ToyIoRegistry.class.getCanonicalName());
        final GraphSONRecordWriter writer = new GraphSONRecordWriter(new DataOutputStream(new FileOutputStream(input)), ConfUtil.makeHadoopConfiguration(graph.configuration()));
        validateIoRegistryGraph(graph, graphComputerClass, writer);
        assertTrue(input.delete());
    }

    private void validateIoRegistryGraph(final HadoopGraph graph,
                                         final Class<? extends GraphComputer> graphComputerClass,
                                         final RecordWriter<NullWritable, VertexWritable> writer) throws Exception {


        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            final StarGraph starGraph = StarGraph.open();
            Vertex vertex = starGraph.addVertex(T.label, "place", T.id, i, "point", new ToyPoint(i, i * 10), "message", "I'm " + i, "triangle", new ToyTriangle(i, i * 10, i * 100));
            vertex.addEdge("connection", starGraph.addVertex(T.id, i > 0 ? i - 1 : NUMBER_OF_VERTICES - 1));
            writer.write(NullWritable.get(), new VertexWritable(starGraph.getStarVertex()));
        }
        writer.close(new TaskAttemptContextImpl(ConfUtil.makeHadoopConfiguration(graph.configuration()), new TaskAttemptID()));

        // OLAP TESTING //
        validatePointTriangles(graph.traversal().withComputer(graphComputerClass).V().project("point", "triangle").by("point").by("triangle").toList());
        validatePointTriangles(graph.traversal().withComputer(graphComputerClass).V().out().project("point", "triangle").by("point").by("triangle").toList());
        validatePointTriangles(graph.traversal().withComputer(graphComputerClass).V().out().out().project("point", "triangle").by("point").by("triangle").toList());
        // OLTP TESTING //
        validatePointTriangles(graph.traversal().V().project("point", "triangle").by("point").by("triangle").toList());
        // HDFS TESTING //
        /*validatePointTriangles(IteratorUtils.<Map<String, Object>>asList(IteratorUtils.<Vertex, Map<String, Object>>map(FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration())).head(graph.configuration().getInputLocation(), graph.configuration().getGraphReader()),
                vertex -> {
                    return new HashMap<String, Object>() {{
                        put("point", vertex.value("point"));
                        put("triangle", vertex.value("triangle"));
                    }};
                })));*/
    }

    private void validatePointTriangles(final List<Map<String, Object>> values) {
        assertEquals(NUMBER_OF_VERTICES, values.size());
        for (int i = 0; i < NUMBER_OF_VERTICES; i++) {
            assertTrue(values.stream().map(m -> m.get("point")).collect(Collectors.toList()).contains(new ToyPoint(i, i * 10)));
            assertTrue(values.stream().map(m -> m.get("triangle")).collect(Collectors.toList()).contains(new ToyTriangle(i, i * 10, i * 100)));
        }
    }
}
