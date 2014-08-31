package com.tinkerpop.gremlin.structure.server;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Partition<V extends Comparable<V>> {

    /**
     * The priority specifies the priority this partition has in answering queries for vertices/edges that fall
     * in this range.
     */
    public int getPriority();

    public List<VertexRange<V>> getVertexRanges();

}
