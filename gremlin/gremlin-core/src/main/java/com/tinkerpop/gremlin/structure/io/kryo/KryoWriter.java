package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.AnnotatedList;
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
import java.util.Map;
import java.util.Optional;

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
    public void writeGraph(final OutputStream outputStream) throws IOException {
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
            writeVertexToOutput(output, v, Direction.OUT);
        }

        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        final Output output = new Output(outputStream);
        writeVertexToOutput(output, v, direction);
        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        final Output output = new Output(outputStream);
        writeVertexWithNoEdgesToOutput(output, v);
        output.flush();
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        final Output output = new Output(outputStream);
        kryo.writeClassAndObject(output, e.getVertex(Direction.OUT).getId());
        kryo.writeClassAndObject(output, e.getVertex(Direction.IN).getId());
        writeEdgeToOutput(output, e);
        output.flush();
    }

    private void writeEdgeToOutput(final Output output, final Edge e) {
        this.writeElement(output, e, Optional.empty());
    }

    private void writeVertexWithNoEdgesToOutput(final Output output, final Vertex v) {
        writeElement(output, v, Optional.empty());
    }

    private void writeVertexToOutput(final Output output, final Vertex v, final Direction direction) {
        this.writeElement(output, v, Optional.of(direction));
    }

    private void writeElement(final Output output, final Element e, final Optional<Direction> direction) {
        kryo.writeClassAndObject(output, e.getId());
        output.writeString(e.getLabel());

        writeProperties(output, e);

        if (e instanceof Vertex) {
            output.writeBoolean(direction.isPresent());
            if (direction.isPresent()) {
                final Vertex v = (Vertex) e;
                final Direction d = direction.get();

                if (d == Direction.BOTH || d == Direction.OUT)
                    writeDirectionalEdges(output, d, v.outE());

                if (d == Direction.BOTH || d == Direction.IN)
                    writeDirectionalEdges(output, d, v.inE());
            }

            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }
    }

    private void writeDirectionalEdges(final Output output, final Direction d, final Iterator<Edge> vertexEdges) {
        final boolean hasEdges = vertexEdges.hasNext();
        output.writeBoolean(hasEdges);
        if (hasEdges)
            kryo.writeObject(output, d);

        while (vertexEdges.hasNext()) {
            final Edge edgeToWrite = vertexEdges.next();
            kryo.writeClassAndObject(output, edgeToWrite.getVertex(d.opposite()).getId());
            writeEdgeToOutput(output, edgeToWrite);
        }
    }

    private void writeProperties(final Output output, final Element e) {
        final Map<String, Property> properties = e.getProperties();
        final int propertyCount = properties.size();
        output.writeInt(propertyCount);
        properties.forEach((key,val) -> {
            output.writeString(key);
            writePropertyValue(output, val);
        });
    }

    private void writePropertyValue(final Output output, final Property val) {
        if (val.get() instanceof AnnotatedList)
            kryo.writeClassAndObject(output, KryoAnnotatedList.from((AnnotatedList) val.get()));
        else
            kryo.writeClassAndObject(output, val.get());
    }
}
