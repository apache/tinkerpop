package com.tinkerpop.gremlin.structure.server;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ClusterAware<V extends Comparable<V>> {
    public List<Partition<V>> getVertexPartitions();
}
