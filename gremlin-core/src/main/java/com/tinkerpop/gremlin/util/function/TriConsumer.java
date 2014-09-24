package com.tinkerpop.gremlin.util.function;

import java.util.Objects;

/**
 * Represents an operation that accepts two input arguments and returns no result. This is the tri-arity
 * specialization of {@link java.util.function.Consumer}. Unlike most other functional interfaces, {@link TriConsumer}
 * is expected to operate via side-effects.
 * <p/>
 * This is a functional interface whose functional method is {@link #accept(Object, Object, Object)}.
 *
 * @param <A> the type of the first argument to the operation
 * @param <B> the type of the second argument to the operation
 * @param <C> the type of the third argument to the operation
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface TriConsumer<A, B, C> {

    /**
     * Performs this operation on the given arguments.
     *
     * @param a the first argument to the operation
     * @param b the second argument to the operation
     * @param c the third argument to the operation
     */
    public void accept(final A a, final B b, final C c);

    /**
     * Returns a composed @{link TriConsumer} that performs, in sequence, this operation followed by the {@code after}
     * operation. If performing either operation throws an exception, it is relayed to the caller of the composed
     * operation. If performing this operation throws an exception, the after operation will not be performed.
     *
     * @param after the operation to perform after this operation
     * @return a composed {@link TriConsumer} that performs in sequence this operation followed by the {@code after}
     * operation
     * @throws NullPointerException if {@code after} is null
     */
    public default TriConsumer<A, B, C> andThen(final TriConsumer<? super A, ? super B, ? super C> after) {
        Objects.requireNonNull(after);
        return (A a, B b, C c) -> {
            accept(a, b, c);
            after.accept(a, b, c);
        };
    }
}
