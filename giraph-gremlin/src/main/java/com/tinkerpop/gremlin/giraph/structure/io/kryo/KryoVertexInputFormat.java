package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.structure.io.GiraphGremlinInputFormat;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class KryoVertexInputFormat extends VertexInputFormat implements GiraphGremlinInputFormat {

    private final KryoInputFormat fileInputFormat;

    public KryoVertexInputFormat() {
        fileInputFormat = new KryoInputFormat();
    }

    @Override
    public List<InputSplit> getSplits(final JobContext context,
                                      final int minSplitCountHint) throws IOException, InterruptedException {
        // note: hint ignored
        return fileInputFormat.getSplits(context);
    }

    @Override
    public VertexReader createVertexReader(final InputSplit split,
                                           final TaskAttemptContext context) throws IOException {
        VertexReader reader = new KryoVertexReader();
        try {
            reader.initialize(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return reader;
    }

    @Override
    public Class<InputFormat> getInputFormatClass() {
        return (Class) KryoInputFormat.class;
    }
}
