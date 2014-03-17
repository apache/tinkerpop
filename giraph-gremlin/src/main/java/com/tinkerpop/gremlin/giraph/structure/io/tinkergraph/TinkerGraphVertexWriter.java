package com.tinkerpop.gremlin.giraph.structure.io.tinkergraph;


import com.tinkerpop.gremlin.giraph.structure.GiraphVertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoWriter;
import com.tinkerpop.tinkergraph.TinkerGraph;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraphVertexWriter extends VertexWriter {

    KryoWriter writer;

    public void initialize(final TaskAttemptContext context) {
        this.writer = new KryoWriter.Builder(TinkerGraph.open()).build();
    }

    public void writeVertex(final Vertex vertex) throws IOException {
        System.out.println(((GiraphVertex) vertex).getGremlinVertex());
    }

    public void close(final TaskAttemptContext context) {

    }
}
