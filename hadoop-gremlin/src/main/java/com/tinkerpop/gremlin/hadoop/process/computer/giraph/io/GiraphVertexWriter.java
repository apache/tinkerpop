package com.tinkerpop.gremlin.hadoop.process.computer.giraph.io;

import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphComputeVertex;
import com.tinkerpop.gremlin.hadoop.structure.hdfs.VertexWritable;
import com.tinkerpop.gremlin.hadoop.structure.io.kryo.KryoOutputFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexWriter extends VertexWriter {
    private final KryoOutputFormat outputFormat;
    private RecordWriter<NullWritable, VertexWritable> recordWriter;

    public GiraphVertexWriter() {
        outputFormat = new KryoOutputFormat();
    }

    @Override
    public void initialize(TaskAttemptContext context) throws IOException, InterruptedException {
        recordWriter = outputFormat.getRecordWriter(context);
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        recordWriter.close(context);
    }

    @Override
    public void writeVertex(Vertex vertex) throws IOException, InterruptedException {
        recordWriter.write(NullWritable.get(), new VertexWritable(((GiraphComputeVertex) vertex).getBaseVertex()));
    }
}
