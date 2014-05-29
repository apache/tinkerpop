package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONAdjacencyInputFormat;
import com.tinkerpop.gremlin.giraph.structure.io.graphson.GraphSONAdjacencyVertexReader;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GremlinKryoVertexInputFormat extends VertexInputFormat {

    private final GremlinKryoInputFormat fileInputFormat;

    public GremlinKryoVertexInputFormat() {
        fileInputFormat = new GremlinKryoInputFormat();
    }

    public List<InputSplit> getSplits(final JobContext context,
                                      final int minSplitCountHint) throws IOException, InterruptedException {
        // note: hint ignored
        return fileInputFormat.getSplits(context);
    }

    public VertexReader createVertexReader(final InputSplit split,
                                           final TaskAttemptContext context) throws IOException {
        VertexReader reader = new GremlinKryoVertexReader();
        try {
            reader.initialize(split, context);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return reader;
    }
}
