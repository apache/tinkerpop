package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONAdjacencyRecordReaderTest {
    @Test
    public void testAll() throws Exception {
        Configuration conf = new Configuration(false);
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        conf.set("fs.default.name", "file:///");

        File testFile = new File(this.getClass().getResource("grateful-dead-adjlist.json").getPath());
        FileSplit split = new FileSplit(
                new Path(testFile.getAbsoluteFile().toURI().toString()), 0,
                testFile.length(), null);
        System.out.println("reading GraphSON adjacency file " + testFile.getAbsolutePath() + " (" + testFile.length() + " bytes)");

        GraphSONAdjacencyInputFormat inputFormat = ReflectionUtils.newInstance(GraphSONAdjacencyInputFormat.class, conf);
        RecordReader reader = inputFormat.createRecordReader(split,
                new TaskAttemptContext(conf, new TaskAttemptID()));

        float lastProgress = -1f;
        int count = 0;
        boolean foundKeyValue = false;
        while (reader.nextKeyValue()) {
            //System.out.println("" + reader.getProgress() + "> " + reader.getCurrentKey() + ": " + reader.getCurrentValue());
            count++;
            float progress = reader.getProgress();
            assertTrue(progress >= lastProgress);
            assertEquals(NullWritable.class, reader.getCurrentKey().getClass());
            GiraphVertex v = (GiraphVertex) reader.getCurrentValue();
            Vertex vertex = v.getGremlinVertex();
            assertEquals(String.class, vertex.id().getClass());

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

    private <T> long count(final Iterable<T> iter) {
        long count = 0;
        for (T anIter : iter) {
            count++;
        }

        return count;
    }
}
