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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GryoRecordReaderWriterTest {
    @Test
    public void testAll() throws Exception {
        final Configuration configuration = new Configuration(false);
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        configuration.set("fs.default.name", "file:///");

        final File testFile = new File(HadoopGraphProvider.PATHS.get("grateful-dead-vertices.kryo"));
        final int numberOfSplits = 4;
        final long testFileSize = testFile.length();
        final long splitLength = (long) ((double) testFileSize / (double) numberOfSplits);
        //System.out.println("Test file size: " + testFileSize);
        //System.out.println("Test file split length: " + splitLength);
        final List<FileSplit> splits = new ArrayList<>();
        for (int i = 0; i < testFileSize; i = i + (int) splitLength + 1) {
            splits.add(new FileSplit(new Path(testFile.getAbsoluteFile().toURI().toString()), i, splitLength, null));
        }


        final List<String> writeLines = new ArrayList<>();
        final GryoInputFormat inputFormat = ReflectionUtils.newInstance(GryoInputFormat.class, configuration);
        final TaskAttemptContext job = new TaskAttemptContext(configuration, new TaskAttemptID());
        int vertexCount = 0;
        int outEdgeCount = 0;
        int inEdgeCount = 0;
        boolean foundKeyValue = false;
        for (final FileSplit split : splits) {
            System.out.println("reading Gryo file split " + testFile.getAbsolutePath() + " (" + split.getStart() + "--to-->" + (split.getStart() + split.getLength()) + " bytes)");
            final RecordReader reader = inputFormat.createRecordReader(split, job);
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try (final DataOutputStream dos = new DataOutputStream(bos)) {
                final GryoOutputFormat outputFormat = new GryoOutputFormat();
                final RecordWriter writer = outputFormat.getRecordWriter(job, dos);
                float lastProgress = -1f;
                while (reader.nextKeyValue()) {
                    //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
                    final float progress = reader.getProgress();
                    assertTrue(progress >= lastProgress);
                    assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
                    final VertexWritable v = (VertexWritable) reader.getCurrentValue();
                    writer.write(NullWritable.get(), v);
                    vertexCount++;
                    outEdgeCount = outEdgeCount + (int) IteratorUtils.count(v.get().edges(Direction.OUT));
                    inEdgeCount = inEdgeCount + (int) IteratorUtils.count(v.get().edges(Direction.IN));

                    final Vertex vertex = v.get();
                    assertEquals(Integer.class, vertex.id().getClass());

                    final Object value = vertex.property("name");
                    if (((Property) value).value().equals("SUGAR MAGNOLIA")) {
                        foundKeyValue = true;
                        assertEquals(92, IteratorUtils.count(vertex.edges(Direction.OUT)));
                        assertEquals(77, IteratorUtils.count(vertex.edges(Direction.IN)));
                    }

                    lastProgress = progress;
                }
                writeLines.addAll(Arrays.asList(new String(bos.toByteArray()).split("\\x3a\\x15.\\x11\\x70...")));
            }
        }
        assertEquals(8049,outEdgeCount);
        assertEquals(8049,inEdgeCount);
        assertEquals(outEdgeCount,inEdgeCount);
        assertEquals(808, vertexCount);
        assertTrue(foundKeyValue);
        assertEquals(808, writeLines.size());
        final String line42 = writeLines.get(41);
        assertTrue(line42.contains("ITS ALL OVER NO"));
    }
}
