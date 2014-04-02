package com.tinkerpop.gremlin.util.function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface ThrowingSupplier<T> {
    public T get() throws Exception;
}
