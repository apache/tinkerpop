package com.tinkerpop.gremlin.process.computer;

import org.javatuples.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Memory {

    public default boolean exists(final String key) {
        return this.keys().contains(key);
    }

    public Set<String> keys();

    public <R> R get(final String key) throws IllegalArgumentException;

    public void set(final String key, Object value);

    public default Map<String, Object> asMap() {
        final Map<String, Object> map = keys().stream()
                .filter(this::exists)
                .map(key -> Pair.with(key, get(key)))
                .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
        return Collections.unmodifiableMap(map);
    }

    public int getIteration();

    public long getRuntime();

    public long incr(final String key, final long delta);

    public boolean and(final String key, final boolean bool);

    public boolean or(final String key, final boolean bool);

    public default boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    public interface Administrative extends Memory {

        public void incrIteration();

        public void setRuntime(final long runtime);
    }


    public static class Exceptions {

        public static IllegalArgumentException memoryKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Graph computer memory key can not be the empty string");
        }

        public static IllegalArgumentException memoryKeyCanNotBeNull() {
            return new IllegalArgumentException("Graph computer memory key can not be null");
        }

        public static IllegalArgumentException memoryValueCanNotBeNull() {
            return new IllegalArgumentException("Graph computer memory value can not be null");
        }

        public static IllegalStateException memoryCompleteAndImmutable() {
            return new IllegalStateException("Graph computer memory is complete and immutable");
        }

        public static IllegalArgumentException memoryDoesNotExist(final String key) {
            return new IllegalArgumentException("The memory does not have a value for provided key: " + key);
        }

        public static UnsupportedOperationException dataTypeOfMemoryValueNotSupported(final Object val) {
            return new UnsupportedOperationException(String.format("Graph computer memory value [%s] is of type %s is not supported", val, val.getClass()));
        }
    }

}
