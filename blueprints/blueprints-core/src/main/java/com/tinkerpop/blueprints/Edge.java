package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public String getLabel();
}
