package com.tinkerpop.gremlin.util.function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface ThrowingFunction<T, R> {
    R apply(final T t) throws Exception;
}
