package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Vertex extends Element {

    public VertexQuery query();

    public Edge addEdge(String label, Vertex inVertex, Property... properties);
}
