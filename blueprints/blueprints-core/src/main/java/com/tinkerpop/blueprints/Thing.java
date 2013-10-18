package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Thing {

    // public <T extends Features> T getFeatures();

    public <T> Property<T, ? extends Thing> getProperty(String key);

    public <T> Property<T, ? extends Thing> setProperty(String key, T value);

    public <T> Property<T, ? extends Thing> removeProperty(String key);
}
