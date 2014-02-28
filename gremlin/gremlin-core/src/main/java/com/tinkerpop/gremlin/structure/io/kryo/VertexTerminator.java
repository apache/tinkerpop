package com.tinkerpop.gremlin.structure.io.kryo;

/**
 * Represents the end of a vertex in a serialization stream.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class VertexTerminator {
    public static final VertexTerminator INSTANCE = new VertexTerminator();
    private final boolean terminal;

    private VertexTerminator() {
        this.terminal = true;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final VertexTerminator that = (VertexTerminator) o;

        if (terminal != that.terminal) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (terminal ? 1 : 0);
    }
}
