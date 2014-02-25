package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

        final boolean supportsAnnotations = graph.getFeatures().graph().supportsAnnotations();
        output.writeBoolean(supportsAnnotations);
        if (supportsAnnotations)
            kryo.writeObject(output, graph.annotations().getAnnotations());

        final Iterator<Vertex> vertices = graph.V();
        final boolean hasSomeVertices = vertices.hasNext();
        output.writeBoolean(hasSomeVertices);
        while (vertices.hasNext()) {
            final Vertex v = vertices.next();
            writeElement(output, v, true);
        }

        output.flush();
    }

    private void writeElement(final Output output, final Element e, final boolean isVertex) {

        // todo: portability of ids???
        kryo.writeClassAndObject(output, e.getId());
        output.writeString(e.getLabel());

        writeProperties(output, e);

        if (isVertex) {
            final Vertex v = (Vertex) e;
            final Iterator<Edge> vertexEdgesOut = v.outE();
            output.writeBoolean(vertexEdgesOut.hasNext());
            while (vertexEdgesOut.hasNext()) {
                final Edge edgeToWrite = vertexEdgesOut.next();
                kryo.writeClassAndObject(output, edgeToWrite.getVertex(Direction.IN).getId());
                writeElement(output, edgeToWrite, false);
            }

            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }
    }

    private void writeProperties(final Output output, final Element e) {
        final Map<String, Property> properties = e.getProperties();
        final int propertyCount = properties.size();
        output.writeInt(propertyCount);
        properties.forEach((key,val) -> {
            output.writeString(key);
            writePropertyValue(output, key, val);
        });
    }

    private void writePropertyValue(final Output output, final String key, final Property val) {
        if (!(val instanceof AnnotatedList))
            kryo.writeClassAndObject(output, val.get());
        else {
            final AnnotatedList alist = (AnnotatedList) val;
            final List<AnnotatedValue> avalues = alist.annotatedValues().toList();
            output.writeInt(avalues.size());
            avalues.forEach(av -> {
                output.writeString(key);
                kryo.writeClassAndObject(output, av.getValue());

                final Set<String> annotationKeys = av.getAnnotationKeys();
                output.write(annotationKeys.size());

                annotationKeys.forEach(ak -> {
                    output.writeString(ak);
                    kryo.writeClassAndObject(output, av.getAnnotation(ak));
                });
            });
        }
    }
}
