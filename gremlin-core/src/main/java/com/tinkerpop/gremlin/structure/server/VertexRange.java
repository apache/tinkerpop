package com.tinkerpop.gremlin.structure.server;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexRange<V extends Comparable<V>> {
    public boolean contains(final V item);

    public V getStartRange();

    public V getEndRange();
}
