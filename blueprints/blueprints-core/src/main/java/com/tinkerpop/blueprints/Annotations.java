package com.tinkerpop.blueprints;

import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Annotations {

    public class Key {

        public static final String VALUE = "value";
        private static final String HIDDEN_PREFIX = "%&%";

        public static String hidden(final String key) {
            return HIDDEN_PREFIX.concat(key);
        }
    }

    public void set(final String key, final Object value);

    public <T> Optional<T> get(final String key);

    public Set<String> getKeys();

}