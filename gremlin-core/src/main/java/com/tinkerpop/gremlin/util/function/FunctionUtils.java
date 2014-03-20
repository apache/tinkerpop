package com.tinkerpop.gremlin.util.function;

import java.util.function.Function;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class FunctionUtils {
    private FunctionUtils() {
    }

    public static <T, U> Function<T,U> wrap(final ThrowingFunction<T, U> functionThatThrows) {
        return (a) -> {
            try {
                return functionThatThrows.apply(a);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }


}
