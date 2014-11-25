package com.tinkerpop.gremlin.hadoop.process.computer.giraph.io;

import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphComputeVertex;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexWriter extends VertexWriter {
    private final OutputFormat<NullWritable, VertexWritable> outputFormat;
    private RecordWriter<NullWritable, VertexWritable> recordWriter;

    public GiraphVertexWriter(final OutputFormat<NullWritable, VertexWritable> outputFormat) {
        this.outputFormat = outputFormat;
    }

    @Override
    public void initialize(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordWriter = this.outputFormat.getRecordWriter(context);
    }

    @Override
    public void close(final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordWriter.close(context);
    }

    @Override
    public void writeVertex(final Vertex vertex) throws IOException, InterruptedException {
        this.recordWriter.write(NullWritable.get(), new VertexWritable(((GiraphComputeVertex) vertex).getBaseVertex()));
    }
}
