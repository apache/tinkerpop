package com.tinkerpop.gremlin.util.function;

import java.util.Objects;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TriFunction<A, B, C, R> {

    /**
     * Applies this function to the given arguments.
     *
     * @param a the first argument to the function
     * @param b the second argument to the function
     * @param c the third argument to the function
     * @return the function result
     */
    public R apply(final A a, final B b, final C c);

    /**
     * Returns a composed function that first applies this function to its input, and then applies the after function
     * to the result. If evaluation of either function throws an exception, it is relayed to the caller of the composed
     * function.
     *
     * @param after the function to apply after this function is applied
     * @param <V>   the type of the output of the {@code after} function, and of the composed function
     * @return a composed function that first applies this function and then applies the {@code after} function.
     * @throws NullPointerException if {@code after} is null
     */
    public default <V> TriFunction<A, B, C, V> andThen(final Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (A a, B b, C c) -> after.apply(apply(a, b, c));
    }
}