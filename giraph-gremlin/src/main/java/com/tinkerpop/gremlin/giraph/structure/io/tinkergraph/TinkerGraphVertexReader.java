package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;

import com.tinkerpop.gremlin.giraph.process.olap.GiraphGraphComputer;
import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        final FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        final TinkerGraph g = TinkerGraph.open();
        GraphMLReader.create().build().readGraph(fileSystem.open(new Path(context.getConfiguration().get(GiraphGraphComputer.GREMLIN_INPUT_LOCATION))), g);
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
