package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoReader implements GraphReader {
    private final Kryo kryo = new Kryo();
    private final Graph graph;

    private final File tempFile = new File("/tmp" + File.separator + UUID.randomUUID() + ".tmp");
    private final Map<Object, Object> idMap = new HashMap<>();

    public KryoReader(final Graph g) {
        this.graph = g;
    }
    @Override
    public void inputGraph(final InputStream inputStream) throws IOException {
        final Input input = new Input(inputStream);
        final Output output = new Output(new FileOutputStream(tempFile));

        try {
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

                    idMap.put(current, v.getId());

                    // if there are edges then read them to end and write to temp otherwise, read what should be
                    // the terminator
                    if (!input.readBoolean())
                        kryo.readClassAndObject(input);
                    else {
                        // writes the real new id of the outV to the temp.  only need to write vertices to temp that
                        // have edges.  no need to reprocess those that don't again.
                        kryo.writeClassAndObject(output, v.getId());
                        readToEndOfEdgesAndWriteToTemp(input, output);
                    }

                }
            }
        } finally {
            // done writing to temp
            output.close();
        }

        // start reading in the edges now from the temp file
        final Input edgeInput = new Input(new FileInputStream(tempFile));
        readFromTempEdges(edgeInput);
        input.close();
    }

    private void readToEndOfEdgesAndWriteToTemp(final Input input, final Output output) throws IOException {
        Object inId = kryo.readClassAndObject(input);
        while (!inId.equals(VertexTerminator.INSTANCE)) {
            kryo.writeClassAndObject(output, inId);

            // edge id
            kryo.writeClassAndObject(output, kryo.readClassAndObject(input));

            // label
            output.writeString(input.readString());
            final int props = input.readInt();
            output.writeInt(props);
            IntStream.range(0, props).forEach(i-> {
                // key
                output.writeString(input.readString());

                // value
                kryo.writeClassAndObject(output, kryo.readClassAndObject(input));
            });

            // next inId or terminator
            inId = kryo.readClassAndObject(input);
        }

        kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
    }

    private void readFromTempEdges(final Input input) {
        while (!input.eof()) {
            // in this case the outId is the id assigned by the graph
            final Object outId = kryo.readClassAndObject(input);
            Object inId = kryo.readClassAndObject(input);
            while (!inId.equals(VertexTerminator.INSTANCE)) {
                final Vertex vOut = graph.v(outId);
                final Object edgeId = kryo.readClassAndObject(input);
                final String edgeLabel = input.readString();
                final Vertex inV = graph.v(idMap.get(inId));

                final Edge e;
                if (graph.getFeatures().edge().supportsUserSuppliedIds())
                    e = vOut.addEdge(edgeLabel, inV, Element.ID, edgeId);
                else
                    e = vOut.addEdge(edgeLabel, inV);

                final int props = input.readInt();
                IntStream.range(0, props).forEach(i-> {
                    final String k = input.readString();
                    // todo: annotatedlist
                    e.setProperty(k, kryo.readClassAndObject(input));
                });

                inId = kryo.readClassAndObject(input);
            }
        }
    }
}
