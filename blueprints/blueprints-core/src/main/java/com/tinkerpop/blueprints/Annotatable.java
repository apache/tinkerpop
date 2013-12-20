package com.tinkerpop.blueprints;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Annotatable {

    public <V> void setAnnotation(final String key, final V value);

    public <V> V getAnnotation(final String key);
}
