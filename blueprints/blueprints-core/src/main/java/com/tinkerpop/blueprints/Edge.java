package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public String getLabel();

    public <V> Property<V, Edge> getProperty(String key);

    public <V> Property<V, Edge> setProperty(String key, V value);

    public <V> Property<V, Edge> removeProperty(String key);

    public static Edge.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }
    }

}
