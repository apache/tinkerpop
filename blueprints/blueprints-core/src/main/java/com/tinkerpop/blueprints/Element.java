package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Element {

    public Object getId();

    public <T> Property<T> getProperty(String key);

    public <T> Property<T> setProperty(String key, T value);

    public Iterable<Property> getProperties();

    public void remove();

}
