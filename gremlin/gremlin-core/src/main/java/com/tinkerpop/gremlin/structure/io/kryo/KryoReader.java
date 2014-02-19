package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.io.GraphReader;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

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
    }
}
