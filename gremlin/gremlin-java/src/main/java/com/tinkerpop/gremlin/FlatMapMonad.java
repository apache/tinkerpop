package com.tinkerpop.gremlin;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapMonad<S, E> extends Monad<S, E> {

    public Function<S, Iterable<E>> function;
    private Iterator<E> iterator = Arrays.<E>asList().iterator();

    public FlatMapMonad(Monad<?, S> start, Function<S, Iterable<E>> function) {
        super(start);
        this.function = function;
    }

    public E next() {
        if (this.hasNext())
            return iterator.next();
        else
            throw new NoSuchElementException();
    }

    public boolean hasNext() {
        while (true) {
            if (iterator.hasNext())
                return true;
            else {
                if (start.hasNext())
                    iterator = function.apply(start.next()).iterator();
                else
                    return false;
            }
        }
    }
}
