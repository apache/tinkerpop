package com.tinkerpop.gremlin.process.util.traversal.lambda;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FilterTraversal<S, E> extends AbstractSingleTraversal<S, E> {

    private boolean filter = true;
    private final Predicate<S> predicate;

    public FilterTraversal(final Predicate<S> predicate) {
        this.predicate = predicate;
    }

    @Override
    public boolean hasNext() {
        return this.filter;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.filter = this.predicate.test(start.get());
    }

    @Override
    public String toString() {
        return this.predicate.toString();
    }
}
