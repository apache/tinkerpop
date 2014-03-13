package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;

import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.tinkergraph.TinkerFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexReader extends VertexReader {

    protected Iterator<com.tinkerpop.gremlin.structure.Vertex> vertices = Collections.emptyIterator();

    public void initialize(final InputSplit inputSplit, final TaskAttemptContext context) throws IOException, InterruptedException {
        final Graph g = TinkerFactory.createClassic();
        this.vertices = g.V();
    }


    public boolean nextVertex() throws IOException, InterruptedException {
        return this.vertices.hasNext();
    }

    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return new GiraphVertex(this.vertices.next());
    }

    public void close() throws IOException {
    }


    public float getProgress() throws IOException, InterruptedException {
        return 1.0f;
    }
}
