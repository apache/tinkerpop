package com.tinkerpop.gremlin.structure.server;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IntervalVertexRange<V extends Comparable<V>> implements VertexRange<V> {

    /**
     * Generated uid on Dec 10 2013
     */
    private static final long serialVersionUID = -71905414131570157L;

    /**
     * Inclusive.
     */
    private final V startRange;

    /**
     * Exclusive
     */
    private final V endRange;

    public IntervalVertexRange(final V startRange, final V endRange, final int priority) {
        this.startRange = startRange;
        this.endRange = endRange;
    }

    @Override
    public V getStartRange() {
        return startRange;
    }

    @Override
    public V getEndRange() {
        return endRange;
    }

    @Override
    public boolean contains(V item) {
        return startRange.compareTo(item) <= 0 && endRange.compareTo(item) > 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final IntervalVertexRange<?> that = (IntervalVertexRange<?>) o;

        if (!endRange.equals(that.endRange)) return false;
        if (!startRange.equals(that.startRange)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = startRange.hashCode();
        result = 31 * result + endRange.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ElementRange[start=" + startRange + ", end=" + endRange + ']';
    }
}
