package com.tinkerpop.gremlin.giraph.structure.io;

import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexInputFormat extends VertexInputFormat {

    public List<InputSplit> getSplits(JobContext context, int minSplitCountHint) throws IOException, InterruptedException {
        return null;
    }

    public VertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return null;
    }
}
