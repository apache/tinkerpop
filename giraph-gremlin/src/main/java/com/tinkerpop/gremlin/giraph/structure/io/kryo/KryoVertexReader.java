package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class KryoVertexReader extends VertexReader {

    private final KryoRecordReader recordReader;

    public KryoVertexReader() {
        recordReader = new KryoRecordReader();
    }

    @Override
    public void initialize(final InputSplit inputSplit,
                           final TaskAttemptContext context) throws IOException, InterruptedException {
        recordReader.initialize(inputSplit, context);
    }

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
        return recordReader.nextKeyValue();
    }

    @Override
    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return recordReader.getCurrentValue();
    }

    @Override
    public void close() throws IOException {
        recordReader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
    }
}
