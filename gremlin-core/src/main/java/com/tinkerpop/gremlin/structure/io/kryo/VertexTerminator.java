package com.tinkerpop.gremlin.structure.io.kryo;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the end of a vertex in a serialization stream.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class VertexTerminator {
    public static final VertexTerminator INSTANCE = new VertexTerminator();

    public final byte[] terminal;

    private VertexTerminator() {
        terminal = ByteBuffer.allocate(8).putLong(4185403236219066774L).array();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VertexTerminator that = (VertexTerminator) o;

        return terminal == that.terminal;

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(terminal);
    }
}
