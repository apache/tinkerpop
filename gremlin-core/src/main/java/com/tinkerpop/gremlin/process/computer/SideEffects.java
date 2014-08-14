package com.tinkerpop.gremlin.process.computer;

import org.javatuples.Pair;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
* @author Marko A. Rodriguez (http://markorodriguez.com)
*/
public interface SideEffects {

    public Set<String> keys();

    public <R> Optional<R> get(final String key);

    public void set(final String key, Object value);

    public default Map<String, Object> asMap() {
        final Map<String, Object> map = keys().stream()
                .map(key -> Pair.with(key, get(key).get()))
                .collect(Collectors.toMap(kv -> kv.getValue0(), Pair::getValue1));
        return Collections.unmodifiableMap(map);
    }

    public int getIteration();

    public long getRuntime();

    //public void setIfAbsent(final String key, final Object value);

    public long incr(final String key, final long delta);

    public boolean and(final String key, final boolean bool);

    public boolean or(final String key, final boolean bool);

    public default boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    public interface Administrative extends SideEffects {

        public void incrIteration();

        public void setRuntime(final long runtime);
    }


    public static class Exceptions {

        public static IllegalArgumentException sideEffectKeyCanNotBeEmpty() {
            return new IllegalArgumentException("Graph computer sideEffect key can not be the empty string");
        }

        public static IllegalArgumentException sideEffectKeyCanNotBeNull() {
            return new IllegalArgumentException("Graph computer sideEffect key can not be null");
        }

        public static IllegalArgumentException sideEffectValueCanNotBeNull() {
            return new IllegalArgumentException("Graph computer sideEffect value can not be null");
        }

        public static UnsupportedOperationException dataTypeOfSideEffectValueNotSupported(final Object val) {
            return new UnsupportedOperationException(String.format("Graph computer sideEffect value [%s] is of type %s is not supported", val, val.getClass()));
        }
    }

}
