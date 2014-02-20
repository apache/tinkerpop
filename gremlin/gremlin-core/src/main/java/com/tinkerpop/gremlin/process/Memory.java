package com.tinkerpop.gremlin.process;

import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Memory {

    public static class Variable {

        private static final String HIDDEN_PREFIX = "%&%";

        public static String hidden(final String key) {
            return HIDDEN_PREFIX.concat(key);
        }
    }

    public <T> void set(final String variable, final T value);

    public <T> T get(final String variable);

    public Set<String> getVariables();

    public default <T> T getOrCreate(final String variable, final Supplier<T> orCreate) {
        if (this.getVariables().contains(variable))
            return this.get(variable);
        else {
            T t = orCreate.get();
            this.set(variable, t);
            return t;
        }
    }
}
