package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Element extends Thing {

    public Object getId();

    public <T> Property<T, ? extends Element> getProperty(String key);

    public <T> Property<T, ? extends Element> setProperty(String key, T value);

    public <T> Property<T, ? extends Element> removeProperty(String key);

    public void remove();

}
