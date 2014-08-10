package com.tinkerpop.gremlin.util.function;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Function;

/**
 * Represents a function that accepts two arguments and produces a result. This is the tri-arity specialization of
 * Function.
 * <p>
 * This is a functional interface whose functional method is apply(Object, Object, Object).
 *
 * @param <A> the type of the first argument in the function
 * @param <B> the type of the second argument in the function
 * @param <C> the type of the third argument in the function
 * @param <D> the type of the fourth argument in the function
 * @param <R> the type of the result of the function
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface SQuadFunction<A, B, C, D, R> extends Serializable {

    /**
     * Applies this function to the given arguments.
     *
     * @param a the first argument to the function
     * @param b the second argument to the function
     * @param c the third argument to the function
     * @param d the fourth argument to the function
     * @return the function result
     */
    public R apply(final A a, final B b, final C c, final D d);

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
    public default <V> SQuadFunction<A, B, C, D, V> andThen(final Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (A a, B b, C c, D d) -> after.apply(apply(a, b, c, d));
    }
}
