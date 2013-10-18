package com.tinkerpop.blueprints;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Element extends Thing {

    public default <T> T getValue(String key) throws NoSuchElementException {
        final Property<T, ? extends Element> property = this.getProperty(key);
        if (property.isPresent())
            return property.getValue();
        else throw new NoSuchElementException();
    }

    public Object getId();

    public Set<String> getPropertyKeys();

    public void remove();

    public <T> Property<T, ? extends Element> getProperty(String key);

    public <T> Property<T, ? extends Element> setProperty(String key, T value);

    public <T> Property<T, ? extends Element> removeProperty(String key);

}
