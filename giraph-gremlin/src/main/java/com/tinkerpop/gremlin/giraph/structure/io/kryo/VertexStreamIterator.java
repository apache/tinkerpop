package com.tinkerpop.gremlin.giraph.structure.io.kryo;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.kryo.KryoReader;
import com.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import com.tinkerpop.gremlin.util.function.SQuintFunction;
import com.tinkerpop.gremlin.util.function.STriFunction;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Joshua Shinavier (http://fortytwo.net)
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
                Graph gLocal = TinkerGraph.open();

                STriFunction<Object, String, Object[], Vertex> vertexMaker = (id, label, props) -> createVertex(gLocal, id, label, props);
                SQuintFunction<Object, Object, Object, String, Object[], Edge> edgeMaker = (id, outId, inId, label, props) -> createEdge(gLocal, id, outId, inId, label, props);

                try (InputStream in = new ByteArrayInputStream(output.toByteArray())) {
                    return reader.readVertex(in, Direction.BOTH, vertexMaker, edgeMaker);
                }
            }
        }

        return null;
    }

    private Vertex createVertex(final Graph g,
                                final Object id,
                                final String label,
                                final Object[] props) {

        Object[] newProps = new Object[props.length + 4];
        System.arraycopy(props, 0, newProps, 0, props.length);
        newProps[props.length] = Element.ID;
        newProps[props.length + 1] = id;
        newProps[props.length + 2] = Element.LABEL;
        newProps[props.length + 3] = label;

        return g.addVertex(newProps);
    }

    private Edge createEdge(final Graph g,
                            final Object id,
                            final Object outId,
                            final Object inId,
                            final String label,
                            final Object[] props) {
        Vertex outV;
        try {
            outV = g.v(outId);
        } catch (final NoSuchElementException e) {
            outV = null;
        }
        if (null == outV) {
            outV = g.addVertex(Element.ID, outId);
        }

        Vertex inV;
        try {
            inV = g.v(inId);
        } catch (final NoSuchElementException e) {
            inV = null;
        }
        if (null == inV) {
            inV = g.addVertex(Element.ID, inId);
        }

        Object[] newProps = new Object[props.length + 2];
        System.arraycopy(props, 0, newProps, 0, props.length);
        newProps[props.length] = Element.ID;
        newProps[props.length + 1] = id;

        return outV.addEdge(label, inV, newProps);
    }
}
