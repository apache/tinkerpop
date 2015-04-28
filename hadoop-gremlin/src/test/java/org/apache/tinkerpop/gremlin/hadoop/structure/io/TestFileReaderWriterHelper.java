/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import com.typesafe.config.ConfigException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TestFileReaderWriterHelper {

    public static List<FileSplit> generateFileSplits(final File file, final int numberOfSplits) {
        final long fileSize = file.length();
        final long splitLength = (long) ((double) fileSize / (double) numberOfSplits);
        final List<FileSplit> splits = new ArrayList<>();
        for (int i = 0; i < fileSize; i = i + (int) splitLength + 1) {
            splits.add(new FileSplit(new Path(file.getAbsoluteFile().toURI().toString()), i, splitLength, null));
        }
        return splits;
    }

    public static void validateFileSplits(final List<FileSplit> fileSplits, final Class<? extends InputFormat<NullWritable,VertexWritable>> inputFormatClass, final Optional<Class<? extends OutputFormat<NullWritable,VertexWritable>>> outFormatClass) throws Exception {
        File outputDirectory = TestHelper.makeTestDataPath(inputFormatClass, "hadoop-record-reader-writer-test");
        final Configuration configuration = new Configuration(false);
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());
        configuration.set("fs.default.name", "file:///");
        configuration.set("mapred.output.dir", "file:///" + outputDirectory.getAbsolutePath());
        final InputFormat inputFormat = ReflectionUtils.newInstance(inputFormatClass, configuration);
        final TaskAttemptContext job = new TaskAttemptContext(configuration, new TaskAttemptID(UUID.randomUUID().toString(), 0, true, 0, 0));

        int vertexCount = 0;
        int outEdgeCount = 0;
        int inEdgeCount = 0;

        final OutputFormat<NullWritable,VertexWritable> outputFormat = outFormatClass.isPresent() ? ReflectionUtils.newInstance(outFormatClass.get(), configuration) : null;
        final RecordWriter<NullWritable,VertexWritable> writer = null == outputFormat ? null : outputFormat.getRecordWriter(job);

        boolean foundKeyValue = false;
        for (final FileSplit split : fileSplits) {
            System.out.println("\treading file split " + split.getPath().getName() + " (" + split.getStart() + "..." + (split.getStart() + split.getLength()) + " bytes)");
            final RecordReader reader = inputFormat.createRecordReader(split, job);

            float lastProgress = -1f;
            while (reader.nextKeyValue()) {
                //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
                final float progress = reader.getProgress();
                assertTrue(progress >= lastProgress);
                assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
                final VertexWritable vertexWritable = (VertexWritable) reader.getCurrentValue();
                if (null != writer) writer.write(NullWritable.get(), vertexWritable);
                vertexCount++;
                outEdgeCount = outEdgeCount + (int) IteratorUtils.count(vertexWritable.get().edges(Direction.OUT));
                inEdgeCount = inEdgeCount + (int) IteratorUtils.count(vertexWritable.get().edges(Direction.IN));
                //
                final Vertex vertex = vertexWritable.get();
                assertEquals(Integer.class, vertex.id().getClass());
                if (vertex.value("name").equals("SUGAR MAGNOLIA")) {
                    foundKeyValue = true;
                    assertEquals(92, IteratorUtils.count(vertex.edges(Direction.OUT)));
                    assertEquals(77, IteratorUtils.count(vertex.edges(Direction.IN)));
                }
                lastProgress = progress;
            }
        }
        assertEquals(8049, outEdgeCount);
        assertEquals(8049, inEdgeCount);
        assertEquals(outEdgeCount, inEdgeCount);
        assertEquals(808, vertexCount);
        assertTrue(foundKeyValue);

        if (null != writer) {
            writer.close(new TaskAttemptContext(configuration, job.getTaskAttemptID()));
            for (int i = 1; i < 10; i++) {
                validateFileSplits(generateFileSplits(new File(outputDirectory.getAbsoluteFile() + "/_temporary/" + job.getTaskAttemptID().getTaskID().toString().replace("task", "_attempt") + "_0" + "/part-m-00000"), i), inputFormatClass, Optional.empty());
            }
        }
    }
}
