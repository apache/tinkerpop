package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;

/**
 * The {@link GraphWriter} for the Gremlin Structure serialization format based on Kryo.  The format is meant to be
 * non-lossy in terms of Gremlin Structure to Gremlin Structure migrations (assuming both structure implementations
 * support the same graph features).
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class KryoWriter implements GraphWriter {
    private Kryo kryo;
    private final GremlinKryo.HeaderWriter headerWriter;
    private static final UUID delimiter = UUID.fromString("2DEE3ABF-9963-4546-A578-C1C48690D7F7");
    public static final byte[] DELIMITER = new byte[16];

    static {
        final ByteBuffer bb = ByteBuffer.wrap(DELIMITER);
        bb.putLong(delimiter.getMostSignificantBits());
        bb.putLong(delimiter.getLeastSignificantBits());
    }

    private KryoWriter(final GremlinKryo gremlinKryo) {
        this.kryo = gremlinKryo.createKryo();
        this.headerWriter = gremlinKryo.getHeaderWriter();

    }

    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);

        final boolean supportsGraphMemory = g.features().graph().variables().supportsVariables();
        output.writeBoolean(supportsGraphMemory);
        if (supportsGraphMemory)
            kryo.writeObject(output, new HashMap(g.variables().asMap()));

        final Iterator<Vertex> vertices = g.V();
        final boolean hasSomeVertices = vertices.hasNext();
        output.writeBoolean(hasSomeVertices);
        while (vertices.hasNext()) {
            final Vertex v = vertices.next();
            writeVertexToOutput(output, v, Direction.OUT);
        }

        output.flush();
    }

    @Override
    public void writeGraphNew(final OutputStream outputStream, final Graph g) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);

        final boolean supportsGraphMemory = g.features().graph().variables().supportsVariables();
        output.writeBoolean(supportsGraphMemory);
        if (supportsGraphMemory)
            kryo.writeObject(output, new HashMap(g.variables().asMap()));

        final Iterator<Vertex> vertices = g.V();
        final boolean hasSomeVertices = vertices.hasNext();
        output.writeBoolean(hasSomeVertices);
        while (vertices.hasNext()) {
            final Vertex v = vertices.next();
            writeVertexToOutputNew(output, v, Direction.OUT);
        }

        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        writeVertexToOutput(output, v, direction);
        output.flush();
    }

    @Override
    public void writeVertex(final OutputStream outputStream, final Vertex v) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        writeVertexWithNoEdgesToOutput(output, v);
        output.flush();
    }

    @Override
    public void writeEdge(final OutputStream outputStream, final Edge e) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        kryo.writeClassAndObject(output, e.outV().id().next());
        kryo.writeClassAndObject(output, e.inV().id().next());
        writeEdgeToOutput(output, e);
        output.flush();
    }

    @Override
    public void writeEdgeNew(final OutputStream outputStream, final Edge e) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        kryo.writeClassAndObject(output, DetachedEdge.detach(e));
        output.flush();
    }

    @Override
    public void writeVertexNew(final OutputStream outputStream, final Vertex v, final Direction direction) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        writeVertexToOutputNew(output, v, direction);
        output.flush();
    }

    @Override
    public void writeVertexNew(final OutputStream outputStream, final Vertex v) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);
        writeVertexWithNoEdgesToOutputNew(output, v);
        output.flush();
    }

    private void writeEdgeToOutput(final Output output, final Edge e) {
        this.writeElement(output, e, null);
    }

    private void writeEdgeToOutputNew(final Output output, final Edge e) {
        this.writeElementNew(output, e, null);
    }

    private void writeVertexWithNoEdgesToOutput(final Output output, final Vertex v) {
        writeElement(output, v, null);
    }

    private void writeVertexWithNoEdgesToOutputNew(final Output output, final Vertex v) {
        writeElementNew(output, v, null);
    }

    private void writeVertexToOutput(final Output output, final Vertex v, final Direction direction) {
        this.writeElement(output, v, direction);
    }

    private void writeVertexToOutputNew(final Output output, final Vertex v, final Direction direction) {
        this.writeElementNew(output, v, direction);
    }

    private void writeElement(final Output output, final Element e, final Direction direction) {
        kryo.writeClassAndObject(output, e.id());
        output.writeString(e.label());

        writeProperties(output, e);

        if (e instanceof Vertex) {
            output.writeBoolean(direction != null);
            if (direction != null) {
                final Vertex v = (Vertex) e;
                kryo.writeObject(output, direction);

                if (direction == Direction.BOTH || direction == Direction.OUT)
                    writeDirectionalEdges(output, Direction.OUT, v.outE());

                if (direction == Direction.BOTH || direction == Direction.IN)
                    writeDirectionalEdges(output, Direction.IN, v.inE());
            }

            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }
    }

    private void writeElementNew(final Output output, final Element e, final Direction direction) {
        final DetachedElement detached;
        if (e instanceof Vertex)
            detached = e instanceof DetachedVertex ? (DetachedVertex) e : DetachedVertex.detach((Vertex) e);
        else
            detached = e instanceof DetachedEdge ? (DetachedEdge) e : DetachedEdge.detach((Edge) e);

        // todo: possible to just write object?
        kryo.writeClassAndObject(output, detached);

        if (e instanceof Vertex) {
            output.writeBoolean(direction != null);
            if (direction != null) {
                final Vertex v = (Vertex) e;
                kryo.writeObject(output, direction);

                // todo: use iterators
                if (direction == Direction.BOTH || direction == Direction.OUT)
                    writeDirectionalEdgesNew(output, Direction.OUT, v.outE());

                if (direction == Direction.BOTH || direction == Direction.IN)
                    writeDirectionalEdgesNew(output, Direction.IN, v.inE());
            }

            kryo.writeClassAndObject(output, VertexTerminator.INSTANCE);
        }
    }

    private void writeDirectionalEdges(final Output output, final Direction d, final Iterator<Edge> vertexEdges) {
        final boolean hasEdges = vertexEdges.hasNext();
        kryo.writeObject(output, d);
        output.writeBoolean(hasEdges);

        while (vertexEdges.hasNext()) {
            final Edge edgeToWrite = vertexEdges.next();
            if (d.equals(Direction.OUT))
                kryo.writeClassAndObject(output, edgeToWrite.inV().id().next());
            else if (d.equals(Direction.IN))
                kryo.writeClassAndObject(output, edgeToWrite.outV().id().next());
            writeEdgeToOutput(output, edgeToWrite);
        }

        if (hasEdges)
            kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
    }

    private void writeDirectionalEdgesNew(final Output output, final Direction d, final Iterator<Edge> vertexEdges) {
        final boolean hasEdges = vertexEdges.hasNext();
        kryo.writeObject(output, d);
        output.writeBoolean(hasEdges);

        while (vertexEdges.hasNext()) {
            final Edge edgeToWrite = vertexEdges.next();
            writeEdgeToOutputNew(output, edgeToWrite);
        }

        if (hasEdges)
            kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
    }

    private void writeProperties(final Output output, final Element e) {
        final int propertyCount = (int) StreamFactory.stream(e.iterators().properties()).count();
        output.writeInt(propertyCount);
        e.iterators().properties().forEachRemaining(property -> {
            output.writeString(property.key());
            writePropertyValue(output, property);
        });
        final int hiddenCount = (int) StreamFactory.stream(e.iterators().hiddens()).count();
        output.writeInt(hiddenCount);
        e.iterators().hiddens().forEachRemaining(hidden -> {
            output.writeString(hidden.key());
            writePropertyValue(output, hidden);
        });
    }

    private void writePropertyValue(final Output output, final Property val) {
        kryo.writeClassAndObject(output, val.value());
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Always creates the most current version available.
         */
        private GremlinKryo gremlinKryo = GremlinKryo.build().create();

        private Builder() {
        }

        public Builder custom(final GremlinKryo gremlinKryo) {
            this.gremlinKryo = gremlinKryo;
            return this;
        }

        public KryoWriter create() {
            return new KryoWriter(this.gremlinKryo);
        }
    }
}
