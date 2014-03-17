package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;

import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphOutputFormat extends VertexOutputFormat {

    public void checkOutputSpecs(JobContext context) {

    }

    public VertexWriter createVertexWriter(TaskAttemptContext context) {
        return new TinkerGraphVertexWriter();
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
        return new FileOutputCommitter();
    }


}
