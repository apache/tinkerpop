package com.tinkerpop.gremlin.structure.io.kryo;

/**
 * Represents the end of an edge list in a serialization stream.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class EdgeTerminator {
    public static final EdgeTerminator INSTANCE = new EdgeTerminator();
    private final boolean terminal;

    private EdgeTerminator() {
        this.terminal = true;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EdgeTerminator that = (EdgeTerminator) o;

        return terminal == that.terminal;
    }

    @Override
    public int hashCode() {
        return (terminal ? 1 : 0);
    }
}
