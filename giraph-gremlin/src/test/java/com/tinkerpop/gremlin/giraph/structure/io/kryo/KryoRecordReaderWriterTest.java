package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.GiraphGraphProvider;
import com.tinkerpop.gremlin.giraph.process.computer.GiraphComputeVertex;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class KryoRecordReaderWriterTest {
    @Test
    public void testAll() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.set("fs.default.name", "file:///");

        File testFile = new File(GiraphGraphProvider.PATHS.get("grateful-dead-vertices.gio"));
        FileSplit split = new FileSplit(
                new Path(testFile.getAbsoluteFile().toURI().toString()), 0,
                testFile.length(), null);
        System.out.println("reading Gremlin Kryo file " + testFile.getAbsolutePath() + " (" + testFile.length() + " bytes)");

        KryoInputFormat inputFormat = ReflectionUtils.newInstance(KryoInputFormat.class, conf);
        TaskAttemptContext job = new TaskAttemptContext(conf, new TaskAttemptID());
        RecordReader reader = inputFormat.createRecordReader(split, job);

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(bos)) {
            KryoOutputFormat outputFormat = new KryoOutputFormat();
            RecordWriter writer = outputFormat.getRecordWriter(job, dos);

            float lastProgress = -1f;
            int count = 0;
            boolean foundKeyValue = false;
            while (reader.nextKeyValue()) {
                //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
                count++;
                float progress = reader.getProgress();
                assertTrue(progress >= lastProgress);
                assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
                GiraphComputeVertex v = (GiraphComputeVertex) reader.getCurrentValue();
                writer.write(NullWritable.get(), v);

                Vertex vertex = v.getBaseVertex();
                assertEquals(Integer.class, vertex.id().getClass());

                Object value = vertex.property("name");
                if (null != value && ((Property) value).value().equals("SUGAR MAGNOLIA")) {
                    foundKeyValue = true;
                    assertEquals(92, count(vertex.outE().toList()));
                    assertEquals(77, count(vertex.inE().toList()));
                }

                lastProgress = progress;
            }
            assertEquals(808, count);
            assertTrue(foundKeyValue);
        }

        //System.out.println("bos: " + new String(bos.toByteArray()));
        String[] lines = new String(bos.toByteArray()).split("\\x3a\\x15.\\x11\\x70...");
        assertEquals(808, lines.length);
        String line42 = lines[41];
        //System.out.println("line42: " + line42);
        assertTrue(line42.contains("ITS ALL OVER NO"));
    }

    private <T> long count(final Iterable<T> iter) {
        long count = 0;
        for (T anIter : iter) {
            count++;
        }

        return count;
    }
}
