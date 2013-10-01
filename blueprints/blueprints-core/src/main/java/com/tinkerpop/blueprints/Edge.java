package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public <T> Property<T, Edge> getProperty(String key);

    public <T> Property<T, Edge> setProperty(String key, T value);

    public <T> Property<T, Edge> removeProperty(String key);

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public String getLabel();
}
