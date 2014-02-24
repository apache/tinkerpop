package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoReader implements GraphReader {
    private final Kryo kryo = new Kryo();
    private final Graph graph;

    public KryoReader(final Graph g) {
        this.graph = g;
    }
    @Override
    public void inputGraph(final InputStream inputStream) throws IOException {
        final Input input = new Input(inputStream);

        final boolean supportedAnnotations = input.readBoolean();
        if (supportedAnnotations && graph.getFeatures().graph().supportsAnnotations()) {
            final Graph.Annotations annotations = graph.annotations();
            final Map<String,Object> annotationMap = (Map<String,Object>) kryo.readObject(input, HashMap.class);
            annotationMap.forEach(annotations::set);
        }

        final boolean hasSomeVertices = input.readBoolean();
        if (hasSomeVertices) {
            while (!input.eof()) {
                final Object current = kryo.readClassAndObject(input);
                final Vertex v;
                final String vertexLabel = input.readString();
                if (graph.getFeatures().vertex().supportsUserSuppliedIds())
                    v = graph.addVertex(Element.ID, current, Element.LABEL, vertexLabel);
                else
                    v = graph.addVertex(Element.LABEL, vertexLabel);

                final int numberOfProperties = input.readInt();
                IntStream.range(0, numberOfProperties).forEach(i-> {
                    // todo: do we just let this fail or do we check features for supported property types
                    final String k = input.readString();
                    v.setProperty(k, kryo.readClassAndObject(input));
                });

                final boolean hasEdges = input.readBoolean();
                if (hasEdges) {

                }

                kryo.readClassAndObject(input);
            }
        }
    }
}
