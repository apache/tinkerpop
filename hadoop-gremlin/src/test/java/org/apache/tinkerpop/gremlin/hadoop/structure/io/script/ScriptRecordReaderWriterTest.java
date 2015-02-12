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
package org.apache.tinkerpop.gremlin.hadoop.structure.io.script;

import org.apache.tinkerpop.gremlin.hadoop.HadoopGraphProvider;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
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
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class ScriptRecordReaderWriterTest {

    @Test
    public void testAll() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.set("fs.default.name", "file:///");
        conf.set(ScriptRecordReader.SCRIPT_FILE, HadoopGraphProvider.PATHS.get("script-input.groovy"));
        conf.set(ScriptRecordWriter.SCRIPT_FILE, HadoopGraphProvider.PATHS.get("script-output.groovy"));

        File testFile = new File(HadoopGraphProvider.PATHS.get("tinkerpop-classic.txt"));
        FileSplit split = new FileSplit(
                new Path(testFile.getAbsoluteFile().toURI().toString()), 0,
                testFile.length(), null);
        System.out.println("reading custom formatted adjacency file " + testFile.getAbsolutePath() + " (" + testFile.length() + " bytes)");

        ScriptInputFormat inputFormat = ReflectionUtils.newInstance(ScriptInputFormat.class, conf);
        TaskAttemptContext job = new TaskAttemptContext(conf, new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, job);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            ScriptOutputFormat outputFormat = new ScriptOutputFormat();
            RecordWriter writer = outputFormat.getRecordWriter(job, dos);

            float lastProgress = -1f;
            int count = 0;
            final Set<String> names = new HashSet<>(Arrays.asList("marko", "vadas", "lop", "josh", "ripple", "peter"));
            while (reader.nextKeyValue()) {
                //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
                count++;
                float progress = reader.getProgress();
                assertTrue(progress >= lastProgress);
                assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
                VertexWritable v = (VertexWritable) reader.getCurrentValue();
                writer.write(NullWritable.get(), v);

                Vertex vertex = v.get();
                assertEquals(String.class, vertex.id().getClass());
                assertTrue(vertex.label().equals("person") || vertex.label().equals("project"));

                Property<String> name = vertex.property("name");
                if (name.isPresent()) names.remove(name.value());

                lastProgress = progress;
            }
            assertEquals(6, count);
            assertTrue(names.isEmpty());
        }

        //System.out.println("bos: " + new String(bos.toByteArray()));
        String[] lines = new String(bos.toByteArray()).split("\n");
        assertEquals(6, lines.length);
        String line4 = lines[3];
        assertTrue(line4.startsWith("4:person:josh:32"));
        assertTrue(line4.contains("created:5:1.0"));
        //assertTrue(line4.contains("knows:person:1:1.0")); // TODO: requires edge-copy
    }

    private <T> long count(final Iterable<T> iter) {
        long count = 0;
        for (T anIter : iter) {
            count++;
        }

        return count;
    }
}

