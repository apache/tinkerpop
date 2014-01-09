package com.tinkerpop.blueprints;

import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Annotatable {

    public <V> void setAnnotation(final String key, final V value);

    public <V> Optional<V> getAnnotation(final String key);

    // public boolean hasAnnotation(final String key);

    // public Map<String,Object> getAnnotations();

    // public void removeAnnotation(final String key);

}
