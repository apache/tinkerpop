package com.tinkerpop.blueprints;

import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Edge extends Element {

    public Vertex getVertex(Direction direction) throws IllegalArgumentException;

    public default Set<String> getPropertyKeys() {
        return this.getProperties().keySet();
    }

    public Map<String, Property<?, Edge>> getProperties();

    public <V> Property<V, Edge> getProperty(String key);

    public <V> Property<V, Edge> setProperty(String key, V value);

    public static Edge.Features getFeatures() {
        return new Features() {
        };
    }

    public interface Features extends com.tinkerpop.blueprints.Features {
        public static IllegalArgumentException edgeLabelCanNotBeNull() {
            return new IllegalArgumentException("Edge label can not be null");
        }

        public static IllegalStateException edgePropertiesCanNotHaveProperties() {
            return new IllegalStateException("Edge properties can not have properties");
        }
    }

}
