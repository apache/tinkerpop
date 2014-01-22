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

    public static class Exceptions {
        public static IllegalArgumentException annotationKeyIsReserved(final String key) {
            return new IllegalArgumentException("Annotation key is reserved: " + key);
        }

        public static IllegalArgumentException annotationKeyValueIsReserved() {
            return annotationKeyIsReserved(Key.VALUE);
        }

        public static IllegalArgumentException annotationKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Annotation key can not be the empty string");
        }

        public static IllegalArgumentException annotationKeyCanNotBeNull() {
            return new IllegalArgumentException("Annotation key can not be null");
        }

        public static IllegalArgumentException annotationValueCanNotBeNull() {
            return new IllegalArgumentException("Annotation value can not be null");
        }

        public static UnsupportedOperationException dataTypeOfAnnotationValueNotSupported(final Object val) {
            return new UnsupportedOperationException(String.format("Annotation value [%s] is of type %s is not supported", val, val.getClass()));
        }
    }

}