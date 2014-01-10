package com.tinkerpop.blueprints;

import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Annotations {

    public void set(final String key, final Object value);

    public <T> Optional<T> get(final String key);

    public Set<String> getKeys();

}