package com.tinkerpop.gremlin.structure.server;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface ClusterAware<U extends Comparable> {
    public List<ElementRange<U,Vertex>> getVertexRanges();
    public List<ElementRange<U,Edge>> getEdgeRanges();
}
