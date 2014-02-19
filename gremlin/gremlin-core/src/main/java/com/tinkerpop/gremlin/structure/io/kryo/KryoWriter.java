package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoWriter implements GraphWriter {
    private final Kryo kryo = new Kryo();
    private final Graph graph;

    public KryoWriter(final Graph g) {
        this.graph = g;
    }

    @Override
    public void outputGraph(final OutputStream outputStream) throws IOException {
        final Output output = new Output(outputStream);

        output.writeBoolean(graph.getFeatures().graph().supportsAnnotations());
        if (graph.getFeatures().graph().supportsAnnotations()) {
            kryo.writeObject(output, graph.annotations().getAnnotations());
        }

        output.flush();
    }
}
