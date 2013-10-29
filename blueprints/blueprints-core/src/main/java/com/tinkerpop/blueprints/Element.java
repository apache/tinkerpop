package com.tinkerpop.blueprints;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

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
