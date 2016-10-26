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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
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
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractIoRegistryCheck extends AbstractGremlinTest {

    public void checkGryoIoRegistryCompliance(final HadoopGraph graph, final Class<? extends GraphComputer> graphComputerClass) throws Exception {
        final File input = TestHelper.generateTempFile(this.getClass(), "gryo-io-registry", ".kryo");
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        graph.configuration().setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input.getAbsolutePath());
        graph.configuration().setProperty(GryoPool.CONFIG_IO_REGISTRY, ToyIoRegistry.class.getCanonicalName());
        final GryoRecordWriter writer = new GryoRecordWriter(new DataOutputStream(new FileOutputStream(input)), ConfUtil.makeHadoopConfiguration(graph.configuration()));
        validateIoRegistryGraph(graph, graphComputerClass, writer, GryoInputFormat.class);
        assertTrue(input.delete());
    }

    private void validateIoRegistryGraph(final HadoopGraph graph,
                                         final Class<? extends GraphComputer> graphComputerClass,
                                         final RecordWriter<NullWritable, VertexWritable> writer,
                                         final Class<? extends InputFormat<NullWritable, VertexWritable>> inputFormat) throws Exception {
        for (int i = 0; i < 10; i++) {
            final StarGraph starGraph = StarGraph.open();
            starGraph.addVertex(T.label, "place", T.id, i, "point", new ToyPoint(i, i * 10), "message", "I'm " + i, "triangle", new ToyTriangle(i, i * 10, i * 100));
            writer.write(NullWritable.get(), new VertexWritable(starGraph.getStarVertex()));
        }
        writer.close(new TaskAttemptContextImpl(ConfUtil.makeHadoopConfiguration(graph.configuration()), new TaskAttemptID()));
        // OLAP TESTING //
        final List<Map<String, Object>> values = graph.traversal().withComputer(graphComputerClass).V().valueMap("point", "triangle").toList();
        assertEquals(10, values.size());
        // System.out.println(values);
        for (int i = 0; i < 10; i++) {
            assertTrue(values.stream().map(m -> m.get("point")).flatMap(l -> ((List<ToyPoint>) l).stream()).collect(Collectors.toList()).contains(new ToyPoint(i, i * 10)));
            assertTrue(values.stream().map(m -> m.get("triangle")).flatMap(l -> ((List<ToyTriangle>) l).stream()).collect(Collectors.toList()).contains(new ToyTriangle(i, i * 10, i * 100)));
        }
        values.clear();
        // OLTP TESTING //
        graph.traversal().V().valueMap("point", "triangle").fill(values);
        assertEquals(10, values.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(values.stream().map(m -> m.<List<ToyPoint>>get("point")).flatMap(l -> ((List<ToyPoint>) l).stream()).collect(Collectors.toList()).contains(new ToyPoint(i, i * 10)));
            assertTrue(values.stream().map(m -> m.<List<ToyTriangle>>get("triangle")).flatMap(l -> ((List<ToyTriangle>) l).stream()).collect(Collectors.toList()).contains(new ToyTriangle(i, i * 10, i * 100)));
        }
        values.clear();
        // HDFS TESTING //
        final List<Vertex> list = IteratorUtils.asList(FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration())).head(graph.configuration().getInputLocation(), inputFormat));
        list.forEach(v -> values.add(new HashMap<String, Object>() {{
            put("point", v.value("point"));
            put("triangle", v.value("triangle"));
        }}));
        assertEquals(10, values.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(values.stream().map(m -> m.<ToyPoint>get("point")).collect(Collectors.toList()).contains(new ToyPoint(i, i * 10)));
            assertTrue(values.stream().map(m -> m.<ToyTriangle>get("triangle")).collect(Collectors.toList()).contains(new ToyTriangle(i, i * 10, i * 100)));
        }
    }
}
