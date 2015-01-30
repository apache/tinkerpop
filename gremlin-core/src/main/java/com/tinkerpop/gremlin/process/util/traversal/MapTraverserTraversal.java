package com.tinkerpop.gremlin.process.util.traversal;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapTraverserTraversal<S, E> extends AbstractSingleTraversal<S, E> {

    private E e;
    private final Function<Traverser<S>, E> function;

    public MapTraverserTraversal(final Function<Traverser<S>, E> function) {
        this.function = function;
    }

    @Override
    public E next() {
        return this.e;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.e = this.function.apply(start);
    }

    @Override
    public String toString() {
        return this.function.toString();
    }
}