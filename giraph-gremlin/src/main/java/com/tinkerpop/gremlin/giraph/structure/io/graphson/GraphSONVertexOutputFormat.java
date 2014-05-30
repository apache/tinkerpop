package com.tinkerpop.gremlin.giraph.structure.io.graphson;

import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GraphSONVertexOutputFormat extends VertexOutputFormat {
    public VertexWriter createVertexWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new GraphSONVertexWriter();
    }

    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new FileOutputCommitter();
    }
}
