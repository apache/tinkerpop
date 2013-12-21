package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Blueprints {

    GRAPH, VERTEX, EDGE, PROPERTY, ANNOTATION;

    public static Blueprints of(Object object) {
        if (object instanceof Graph)
            return GRAPH;
        else if (object instanceof Vertex)
            return VERTEX;
        else if (object instanceof Edge)
            return EDGE;
        else if (object instanceof Property)
            return PROPERTY;
        else
            throw new IllegalArgumentException("Unknown type: " + object.getClass());
    }
}
