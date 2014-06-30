package com.tinkerpop.gremlin.util.function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface ThrowingBiConsumer<A, B> {
    public void accept(final A a, final B b) throws Exception;
}
