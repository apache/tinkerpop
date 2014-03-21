package com.tinkerpop.gremlin.util.function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@FunctionalInterface
public interface ThrowingConsumer<Context> {
    public void accept(final Context ctx) throws Exception;
}
