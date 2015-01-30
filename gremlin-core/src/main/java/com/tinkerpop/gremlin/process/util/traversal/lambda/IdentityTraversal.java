package com.tinkerpop.gremlin.process.util.traversal.lambda;

import com.tinkerpop.gremlin.process.Traverser;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IdentityTraversal<S, E> extends AbstractSingleTraversal<S, E> {

    private S s;

    @Override
    public E next() {
        return (E) this.s;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.s = start.get();
    }

    @Override
    public String toString() {
        return "";
    }
}
