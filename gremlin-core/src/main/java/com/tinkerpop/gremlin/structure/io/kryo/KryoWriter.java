package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.GraphWriter;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

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
    private final KryoMapper.HeaderWriter headerWriter;
    private static final UUID delimiter = UUID.fromString("2DEE3ABF-9963-4546-A578-C1C48690D7F7");
    public static final byte[] DELIMITER = new byte[16];

    static {
        final ByteBuffer bb = ByteBuffer.wrap(DELIMITER);
        bb.putLong(delimiter.getMostSignificantBits());
        bb.putLong(delimiter.getLeastSignificantBits());
    }

    private KryoWriter(final KryoMapper kryoMapper) {
        this.kryo = kryoMapper.createMapper();
        this.headerWriter = kryoMapper.getHeaderWriter();
    }

    @Override
    public void writeGraph(final OutputStream outputStream, final Graph g) throws IOException {
        final Output output = new Output(outputStream);
        this.headerWriter.write(kryo, output);

        final boolean supportsGraphMemory = g.features().graph().variables().supportsVariables();
        output.writeBoolean(supportsGraphMemory);
        if (supportsGraphMemory)
            kryo.writeObject(output, new HashMap(g.variables().asMap()));

        final Iterator<Vertex> vertices = g.iterators().vertexIterator();
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
        kryo.writeClassAndObject(output, DetachedFactory.detach(e, true));
        output.flush();
    }

    private void writeEdgeToOutput(final Output output, final Edge e) {
        this.writeElement(output, e, null);
    }

    private void writeVertexWithNoEdgesToOutput(final Output output, final Vertex v) {
        writeElement(output, v, null);
    }

    private void writeVertexToOutput(final Output output, final Vertex v, final Direction direction) {
        this.writeElement(output, v, direction);
    }

    private void writeElement(final Output output, final Element e, final Direction direction) {
        kryo.writeClassAndObject(output, e);

        if (e instanceof Vertex) {
            output.writeBoolean(direction != null);
            if (direction != null) {
                final Vertex v = (Vertex) e;
                kryo.writeObject(output, direction);
                if (direction == Direction.BOTH || direction == Direction.OUT)
                    writeDirectionalEdges(output, Direction.OUT, v.iterators().edgeIterator(Direction.OUT));

                if (direction == Direction.BOTH || direction == Direction.IN)
                    writeDirectionalEdges(output, Direction.IN, v.iterators().edgeIterator(Direction.IN));
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
            writeEdgeToOutput(output, edgeToWrite);
        }

        if (hasEdges)
            kryo.writeClassAndObject(output, EdgeTerminator.INSTANCE);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder {
        /**
         * Always creates the most current version available.
         */
        private KryoMapper kryoMapper = KryoMapper.build().create();

        private Builder() {
        }

        /**
         * Supply a mapper {@link KryoMapper} instance to use as the serializer for the {@code KryoWriter}.
         */
        public Builder mapper(final KryoMapper kryoMapper) {
            this.kryoMapper = kryoMapper;
            return this;
        }

        /**
         * Create the {@code KryoWriter}.
         */
        public KryoWriter create() {
            return new KryoWriter(this.kryoMapper);
        }
    }
}
