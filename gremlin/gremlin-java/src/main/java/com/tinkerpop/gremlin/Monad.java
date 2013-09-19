package com.tinkerpop.gremlin;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class Monad<S, E> {

    protected final Monad<?, S> start;

    public Monad(Monad<?, S> start) {
        this.start = start;
    }

    public <T> Monad<E, T> map(Function<E, T> function) {
        return new MapMonad<>(this, function);
    }

    public <T> Monad<E, T> flatMap(Function<E, Iterable<T>> function) {
        return new FlatMapMonad<E, T>(this, function);
    }

    public Monad<E, E> filter(Predicate<E> predicate) {
        return new FilterMonad<>(this, predicate);
    }

    public abstract E next();

    public abstract boolean hasNext();

    public E orElse(E value) {
        return hasNext() ? next() : value;
    }

    public E orElseGet(Supplier<E> supplier) {
        return hasNext() ? next() : supplier.get();
    }

    public static <T> Monad<T, T> of(final T value) {
        return new Monad<T, T>(null) {
            public T next() {
                return value;
            }

            public boolean hasNext() {
                return true;
            }

        };
    }
}
