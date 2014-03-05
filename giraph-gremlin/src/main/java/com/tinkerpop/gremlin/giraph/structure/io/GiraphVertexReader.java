package com.tinkerpop.gremlin.giraph.structure.io;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GiraphVertexReader extends VertexReader {

    int counter = 0;

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {

    }


    public boolean nextVertex() throws IOException, InterruptedException {
        return counter++ < 100;
    }

    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return null;
    }

    public void close() throws IOException {
    }


    public float getProgress() throws IOException, InterruptedException {
        return 1.0f;
    }
}
