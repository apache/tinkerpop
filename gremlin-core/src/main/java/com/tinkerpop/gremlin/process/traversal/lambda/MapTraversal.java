package com.tinkerpop.gremlin.process.traversal.lambda;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapTraversal<S, E> extends AbstractLambdaTraversal<S, E> {

    private E e;
    private final Function<S, E> function;

    public MapTraversal(final Function<S, E> function) {
        this.function = function;
    }

    @Override
    public E next() {
        return this.e;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.e = this.function.apply(start.get());
    }

    @Override
    public String toString() {
        return this.function.toString();
    }
}
