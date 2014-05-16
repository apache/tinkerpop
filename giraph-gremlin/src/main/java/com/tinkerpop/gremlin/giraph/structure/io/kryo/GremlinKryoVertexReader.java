package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Iterator;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 */
public class GremlinKryoVertexReader extends VertexReader {

    protected Iterator<com.tinkerpop.gremlin.structure.Vertex> vertices = Collections.emptyIterator();
    private Vertex currentVertex = null;
    private float progress = 0f;

    public void initialize(final InputSplit inputSplit,
                           final TaskAttemptContext context) throws IOException, InterruptedException {
        final FileSystem fileSystem = FileSystem.get(context.getConfiguration());

        KryoReader reader = KryoReader.create().build();
        final TinkerGraph g = TinkerGraph.open();

        try (InputStream in = fileSystem.open(new Path(context.getConfiguration().get(GiraphGraphComputer.GREMLIN_INPUT_LOCATION)))) {
            reader.readGraph(in, g);
        }

        this.vertices = g.V();
    }

    public boolean nextVertex() throws IOException, InterruptedException {
        if (this.vertices.hasNext()) {
            currentVertex = new GiraphVertex(this.vertices.next());
            progress = 0.5f;
            return true;
        } else {
            System.out.println("finished reading vertices in GremlinKryoVertexReader");
            currentVertex = null;
            progress = 1f;
            return false;
        }
    }

    public Vertex getCurrentVertex() throws IOException, InterruptedException {
        return currentVertex;
    }

    public void close() throws IOException {
        System.out.println("closing GremlinKryoVertexReader");
        // nothing to do
    }

    public float getProgress() throws IOException, InterruptedException {
        return progress;
    }
}
