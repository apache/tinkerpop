package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GremlinKryoOutputFormat extends VertexOutputFormat {

    public GremlinKryoOutputFormat() {
        System.out.println("create GremlinKryoOutputFormat");
    }

    public void checkOutputSpecs(JobContext context) {

    }

    public VertexWriter createVertexWriter(TaskAttemptContext context) {
        return new GremlinKryoVertexWriter();
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new FileOutputCommitter();
    }
}