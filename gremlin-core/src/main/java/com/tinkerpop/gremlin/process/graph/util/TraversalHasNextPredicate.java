package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalHasNextPredicate<S, E> implements Predicate<Traverser<S>>, Cloneable {

    private Traversal.Admin<S, E> traversal;

    public TraversalHasNextPredicate(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
    }

    @Override
    public boolean test(final Traverser<S> traverser) {
        final Traverser.Admin<S> split = traverser.asAdmin().split();
        split.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(split);
        return this.traversal.hasNext();
    }

    public Traversal<S, E> getTraversal() {
        return this.traversal;
    }

    @Override
    public String toString() {
        return this.traversal.toString();
    }

    @Override
    public TraversalHasNextPredicate<S, E> clone() throws CloneNotSupportedException {
        final TraversalHasNextPredicate<S, E> clone = (TraversalHasNextPredicate<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }
}