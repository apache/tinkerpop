package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONVertexInputFormat extends VertexInputFormat {

    private final GraphSONInputFormat fileInputFormat;

    public GraphSONVertexInputFormat() {
        fileInputFormat = new GraphSONInputFormat();
    }

    public List<InputSplit> getSplits(final JobContext context,
                                      final int minSplitCountHint) throws IOException, InterruptedException {
        // note: hint ignored
        return fileInputFormat.getSplits(context);
    }

    public VertexReader createVertexReader(final InputSplit split,
                                           final TaskAttemptContext context) throws IOException {
        VertexReader reader = new GraphSONVertexReader();
        try {
            reader.initialize(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return reader;
    }
}
