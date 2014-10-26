package com.tinkerpop.gremlin.structure.io.kryo;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

/**
 * An {@link InputStream} implementation that can independently process a Gremlin Kryo file written with
 * {@link KryoWriter#writeVertices(java.io.OutputStream, com.tinkerpop.gremlin.process.Traversal)}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class VertexByteArrayInputStream extends FilterInputStream {

    private static final byte[] vertexTerminatorClass = new byte[]{15, 1, 1, 9};
    private static final byte[] pattern = ByteBuffer.allocate(vertexTerminatorClass.length + 8).put(vertexTerminatorClass).putLong(4185403236219066774L).array();

    public VertexByteArrayInputStream(final InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Read the bytes of the next {@link com.tinkerpop.gremlin.structure.Vertex} in the stream. The returned
     * stream can then be passed to {@link KryoReader#readVertex(java.io.InputStream, java.util.function.Function)}.
     */
    public ByteArrayOutputStream readVertexBytes() throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final LinkedList<Byte> buffer = new LinkedList<>();

        int current = read();
        while (current > -1 && (buffer.size() < 12 || !isMatch(buffer))) {
            stream.write(current);

            current = read();
            if (buffer.size() > 11)
                buffer.removeFirst();

            buffer.addLast((byte) current);
        }

        return stream;
    }

    private static boolean isMatch(final List<Byte> input) {
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != input.get(i)) {
                return false;
            }
        }
        return true;
    }
}
