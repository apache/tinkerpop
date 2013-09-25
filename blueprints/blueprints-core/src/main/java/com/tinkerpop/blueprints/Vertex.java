package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.VertexQuery;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Property... properties);
}
