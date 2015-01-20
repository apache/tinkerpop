package com.tinkerpop.gremlin.util.function;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface CloneableFunction<A, B> extends Function<A, B>, Cloneable {

    public CloneableFunction<A, B> clone() throws CloneNotSupportedException;

    public static <A, B> CloneableFunction<A, B> clone(final CloneableFunction<A, B> cloneableFunction) {
        try {
            return cloneableFunction.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
