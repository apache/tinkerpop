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

    public VertexStreamIterator(final InputStream inputStream,
                                final KryoReader reader) {
        this.inputStream = inputStream;

        this.reader = reader;
    }

    @Override
    public boolean hasNext() {
        if (null != currentVertex) {
            return true;
        } else if (-1 == currentByte) {
            return false;
        } else {
            try {
                currentVertex = advance();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return -1 != currentByte;
        }
    }

    @Override
    public Vertex next() {
        try {
            if (null == currentVertex) {
                if (hasNext()) {
                    try {
                        return advanceToNextVertex();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new IllegalStateException();
                }
            } else {
                return currentVertex;
            }
        } finally {
            currentVertex = null;
            len = 0;
            output.reset();
        }
    }

    private Vertex advanceToNextVertex() throws IOException {
        while (null == currentVertex) {
            currentVertex = advance();
        }

        return currentVertex;
    }

    private Vertex advance() throws IOException {
        currentByte = inputStream.read();

        if (-1 == currentByte) {
            if (len > 0) {
                throw new IllegalStateException("remainder of stream exhausted without matching a vertex");
            } else {
                return null;
            }
        }

        if (len >= BUFLEN) {
            output.write(buffer[len % BUFLEN]);
        }

        buffer[len % BUFLEN] = currentByte;

        len++;

        if (len > BUFLEN) {
            boolean terminated = true;
            for (int i = 0; i < BUFLEN; i++) {
                if (buffer[(len + i) % BUFLEN] != TERMINATOR[i]) {
                    terminated = false;
                    break;
                }
            }

            if (terminated) {
                final Graph gLocal = TinkerGraph.open();
                final Function<DetachedVertex, Vertex> vertexMaker = detachedVertex -> DetachedVertex.addTo(gLocal, detachedVertex);
                final Function<DetachedEdge, Edge> edgeMaker = detachedEdge -> DetachedEdge.addTo(gLocal, detachedEdge);
                try (InputStream in = new ByteArrayInputStream(output.toByteArray())) {
                    return reader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
                }
            }
        }

        return null;
    }
}
