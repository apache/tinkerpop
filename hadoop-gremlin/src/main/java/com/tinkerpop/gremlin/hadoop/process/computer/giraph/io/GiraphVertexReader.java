package com.tinkerpop.gremlin.hadoop.process.computer.giraph.io;

import com.tinkerpop.gremlin.hadoop.process.computer.giraph.GiraphComputeVertex;
import com.tinkerpop.gremlin.hadoop.structure.io.VertexWritable;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexReader extends VertexReader {

    private RecordReader<NullWritable, VertexWritable> recordReader;

    public GiraphVertexReader(final RecordReader<NullWritable, VertexWritable> recordReader) {
        this.recordReader = recordReader;
    }

    @Override
    public void initialize(final InputSplit inputSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
        this.recordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return this.recordReader.nextKeyValue();
    }

    @Override
    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return new GiraphComputeVertex(this.recordReader.getCurrentValue().getBaseVertex());
    }

    @Override
    public void close() throws IOException {
        this.recordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return this.recordReader.getProgress();
    }
}
