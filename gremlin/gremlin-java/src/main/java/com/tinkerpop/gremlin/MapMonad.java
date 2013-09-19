package com.tinkerpop.gremlin;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapMonad<S, E> extends Monad<S, E> {

    private final Function<S, E> function;

    public MapMonad(Monad<?, S> start, Function<S, E> function) {
        super(start);
        this.function = function;
    }

    public E next() {
        return function.apply(this.start.next());
    }

    public boolean hasNext() {
        return this.start.hasNext();
    }
}
