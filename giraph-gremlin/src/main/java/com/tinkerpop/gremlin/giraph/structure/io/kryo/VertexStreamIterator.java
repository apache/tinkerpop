package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexStreamIterator implements Iterator<Vertex> {

    // this is VertexTerminator's long terminal 4185403236219066774L as an array of positive int's
    private static final int[] TERMINATOR = new int[]{58, 21, 138, 17, 112, 155, 153, 150};

    private static int BUFLEN = TERMINATOR.length;

    private final InputStream inputStream;
    private final KryoReader reader;
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final int[] buffer = new int[BUFLEN];

    private int len;
    private int currentByte;
    private Vertex currentVertex;
    private final long maxLength;
    private long currentLength = 0;

    public VertexStreamIterator(final InputStream inputStream, final KryoReader reader) {
        this(inputStream, Long.MAX_VALUE, reader);
    }

    public VertexStreamIterator(final InputStream inputStream, final long maxLength, final KryoReader reader) {
        this.inputStream = inputStream;
        this.reader = reader;
        this.maxLength = maxLength;
    }

    @Override
    public boolean hasNext() {
        if (this.currentLength >= this.maxLength) // gone beyond the split boundary
            return false;
        if (null != this.currentVertex)
            return true;
        else if (-1 == this.currentByte)
            return false;
        else {
            try {
                this.currentVertex = advanceToNextVertex();
                return null != this.currentVertex;
            } catch (final IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    @Override
    public Vertex next() {
        try {
            if (null == this.currentVertex) {
                if (this.hasNext())
                    return this.currentVertex;
                else
                    throw new IllegalStateException("There are no more vertices in this split");
            } else
                return this.currentVertex;
        } finally {
            this.currentVertex = null;
            this.len = 0;
            this.output.reset();
        }
    }

    private final Vertex advanceToNextVertex() throws IOException {
        while (true) {
            this.currentByte = this.inputStream.read();
            this.currentLength++;
            if (-1 == this.currentByte) {
                if (this.len > 0) {
                    throw new IllegalStateException("Remainder of stream exhausted without matching a vertex");
                } else {
                    return null;
                }
            }

            if (this.len >= BUFLEN)
                this.output.write(this.buffer[this.len % BUFLEN]);

            this.buffer[this.len % BUFLEN] = this.currentByte;
            this.len++;

            if (this.len > BUFLEN) {
                boolean terminated = true;
                for (int i = 0; i < BUFLEN; i++) {
                    if (this.buffer[(this.len + i) % BUFLEN] != TERMINATOR[i]) {
                        terminated = false;
                        break;
                    }
                }

                if (terminated) {
                    final Graph gLocal = TinkerGraph.open();
                    final Function<DetachedVertex, Vertex> vertexMaker = detachedVertex -> DetachedVertex.addTo(gLocal, detachedVertex);
                    final Function<DetachedEdge, Edge> edgeMaker = detachedEdge -> DetachedEdge.addTo(gLocal, detachedEdge);
                    try (InputStream in = new ByteArrayInputStream(this.output.toByteArray())) {
                        return this.reader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
                    }
                }
            }
        }
    }
}
