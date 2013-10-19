package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public String getLabel();

    public <T> Property<T, Edge> getProperty(String key);

    public <T> Property<T, Edge> setProperty(String key, T value);

    public <T> Property<T, Edge> removeProperty(String key);

    public default Edge.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {

    }

}
