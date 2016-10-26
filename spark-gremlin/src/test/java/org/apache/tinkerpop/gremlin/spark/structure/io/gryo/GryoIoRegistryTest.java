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

package org.apache.tinkerpop.gremlin.spark.structure.io.gryo;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoOutputFormat;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoRecordWriter;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GryoIoRegistryTest extends AbstractSparkTest {

    @Test
    public void shouldSupportIoRegistry() throws Exception {
        final File input = TestHelper.generateTempFile(this.getClass(), "input", ".kryo");
        final Configuration configuration = super.getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, input.getAbsolutePath());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, GryoOutputFormat.class.getCanonicalName());
        configuration.setProperty(GryoPool.CONFIG_IO_REGISTRY, TestIoRegistry.class.getCanonicalName());
        //configuration.setProperty(Constants.SPARK_SERIALIZER, GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Constants.SPARK_SERIALIZER, KryoSerializer.class.getCanonicalName());
        configuration.setProperty(Constants.SPARK_KRYO_REGISTRATOR, GryoRegistrator.class.getCanonicalName());

        HadoopGraph graph = HadoopGraph.open(configuration);

        final GryoRecordWriter writer = new GryoRecordWriter(new DataOutputStream(new FileOutputStream(input)), ConfUtil.makeHadoopConfiguration(configuration));
        for (int i = 0; i < 10; i++) {
            final StarGraph starGraph = StarGraph.open();
            starGraph.addVertex(T.label, "place", T.id, i, "point", new ToyPoint(i, i * 10), "message", "I'm " + i);
            writer.write(NullWritable.get(), new VertexWritable(starGraph.getStarVertex()));
        }
        writer.close(new TaskAttemptContextImpl(ConfUtil.makeHadoopConfiguration(configuration), new TaskAttemptID()));
        // OLAP TESTING //
        final List<ToyPoint> points = graph.traversal().withComputer(SparkGraphComputer.class).V().<ToyPoint>values("point").toList();
        assertEquals(10, points.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(points.contains(new ToyPoint(i, i * 10)));
        }
        points.clear();
        // OLTP TESTING //
        graph.traversal().V().<ToyPoint>values("point").fill(points);
        assertEquals(10, points.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(points.contains(new ToyPoint(i, i * 10)));
        }
        points.clear();
        // HDFS TESTING //
        final List<Vertex> list = IteratorUtils.asList(FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(configuration)).head(input.getAbsolutePath(), GryoInputFormat.class));
        list.forEach(v -> points.add(v.value("point")));
        assertEquals(10, points.size());
        for (int i = 0; i < 10; i++) {
            assertTrue(points.contains(new ToyPoint(i, i * 10)));
        }
    }
}
