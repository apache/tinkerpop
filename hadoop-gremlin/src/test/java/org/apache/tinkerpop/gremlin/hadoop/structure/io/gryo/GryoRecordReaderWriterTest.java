/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.RecordReaderWriterTest;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoReader;
import org.apache.tinkerpop.shaded.kryo.Kryo;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.UUID;

import static org.junit.Assert.fail;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class GryoRecordReaderWriterTest extends RecordReaderWriterTest {

    @Override
    protected String getInputFilename() {
        return "grateful-dead-v3.kryo";
    }

    @Override
    protected Class<? extends InputFormat<NullWritable, VertexWritable>> getInputFormat() {
        return GryoInputFormat.class;
    }

    @Override
    protected Class<? extends OutputFormat<NullWritable, VertexWritable>> getOutputFormat() {
        return GryoOutputFormat.class;
    }

    /**
     * The reader builds a hardened mapper, i.e. one without the {@code JavaSerializer} backed registrations. The
     * general hardening behavior is covered in gremlin-core's {@code GryoMapperTest}; this pins the wiring here.
     */
    @Test
    public void gryoRecordReaderShouldBuildAHardenedMapper() throws Exception {
        final GryoRecordReader reader = new GryoRecordReader();
        final File file = new File(TestFiles.PATHS.get(getInputFilename()));

        final Configuration config = new Configuration(false);
        config.set("fs.file.impl", LocalFileSystem.class.getName());
        config.set("fs.defaultFS", "file:///");
        final TaskAttemptContext job = new TaskAttemptContextImpl(config,
                new TaskAttemptID(UUID.randomUUID().toString(), 0, TaskType.MAP, 0, 0));

        reader.initialize(new FileSplit(new Path(file.toURI()), 0, file.length(), null), job);

        assertMapperRefusesOptionsStrategy(readerKryo(reader));
    }

    /**
     * The writer builds a hardened mapper on the same terms, kept symmetric with the reader.
     */
    @Test
    public void gryoRecordWriterShouldBuildAHardenedMapper() throws Exception {
        final File outputDirectory = new File(System.getProperty("java.io.tmpdir"),
                "gryo-record-writer-hardening-" + UUID.randomUUID());
        final Configuration config = new Configuration(false);
        config.set("fs.file.impl", LocalFileSystem.class.getName());
        config.set("fs.defaultFS", "file:///");
        config.set("mapreduce.output.fileoutputformat.outputdir", outputDirectory.toURI().toString());
        final TaskAttemptContext job = new TaskAttemptContextImpl(config,
                new TaskAttemptID(UUID.randomUUID().toString(), 0, TaskType.REDUCE, 0, 0));

        final GryoRecordWriter writer = (GryoRecordWriter) new GryoOutputFormat().getRecordWriter(job);
        try {
            assertMapperRefusesOptionsStrategy(writerKryo(writer));
        } finally {
            writer.close(job);
        }
    }

    private static void assertMapperRefusesOptionsStrategy(final Kryo kryo) {
        try {
            kryo.getRegistration(OptionsStrategy.class);
            fail("a hardened Hadoop Gryo mapper must not register OptionsStrategy");
        } catch (IllegalArgumentException expected) {
            // Kryo refuses an unregistered class while registration is required
        }
    }

    private static Kryo readerKryo(final GryoRecordReader reader) throws Exception {
        final Field gryoReaderField = GryoRecordReader.class.getDeclaredField("gryoReader");
        gryoReaderField.setAccessible(true);
        final GryoReader gryoReader = (GryoReader) gryoReaderField.get(reader);
        final Field kryoField = GryoReader.class.getDeclaredField("kryo");
        kryoField.setAccessible(true);
        return (Kryo) kryoField.get(gryoReader);
    }

    private static Kryo writerKryo(final GryoRecordWriter writer) throws Exception {
        final Field gryoWriterField = GryoRecordWriter.class.getDeclaredField("gryoWriter");
        gryoWriterField.setAccessible(true);
        final Object gryoWriter = gryoWriterField.get(writer);
        final Field kryoField = gryoWriter.getClass().getDeclaredField("kryo");
        kryoField.setAccessible(true);
        return (Kryo) kryoField.get(gryoWriter);
    }
}
