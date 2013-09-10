package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Property;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerElement implements Element {

    public Map<String, Object> properties;

    public Object getId() {
        return null;
    }

    public <T> Property<T> getProperty(final String key) {
        return null;
    }

    public <T> Property<T> setProperty(final String key, final T value) {
        return null;
    }

    public Iterable<Property> getProperties() {
        return null;
    }

    public void remove() {

    }


}
