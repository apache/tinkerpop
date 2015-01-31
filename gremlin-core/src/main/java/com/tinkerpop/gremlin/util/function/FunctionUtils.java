package com.tinkerpop.gremlin.util.function;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class FunctionUtils {
    private FunctionUtils() {
    }

    public static <T, U> Function<T, U> wrapFunction(final ThrowingFunction<T, U> functionThatThrows) {
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

    public static <T> Consumer<T> wrapConsumer(final ThrowingConsumer<T> consumerThatThrows) {
        return (a) -> {
            try {
                consumerThatThrows.accept(a);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T,U> BiConsumer<T,U> wrapBiConsumer(final ThrowingBiConsumer<T,U> consumerThatThrows) {
        return (a,b) -> {
            try {
                consumerThatThrows.accept(a,b);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Supplier<T> wrapSupplier(final ThrowingSupplier<T> supplierThatThrows) {
        return () -> {
            try {
                return supplierThatThrows.get();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

}
