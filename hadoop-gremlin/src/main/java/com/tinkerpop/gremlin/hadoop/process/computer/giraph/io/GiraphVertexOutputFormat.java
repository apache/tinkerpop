package com.tinkerpop.gremlin.hadoop.process.computer.giraph.io;

import com.tinkerpop.gremlin.hadoop.Constants;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexOutputFormat extends VertexOutputFormat {

    private OutputFormat<NullWritable, VertexWritable> hadoopGraphOutputFormat;

    @Override
    public VertexWriter createVertexWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        return new GiraphVertexWriter(this.hadoopGraphOutputFormat);
    }

    @Override
    public void checkOutputSpecs(final JobContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        this.hadoopGraphOutputFormat.checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.constructor(context.getConfiguration());
        return this.hadoopGraphOutputFormat.getOutputCommitter(context);
    }

    private final void constructor(final Configuration configuration) {
        if (null == this.hadoopGraphOutputFormat) {
            this.hadoopGraphOutputFormat = ReflectionUtils.newInstance(configuration.getClass(Constants.GREMLIN_HADOOP_GRAPH_OUTPUT_FORMAT, OutputFormat.class, OutputFormat.class), configuration);
        }
    }
}