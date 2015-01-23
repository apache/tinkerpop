package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalNextFunction<S, E> implements Function<Traverser<S>, E>, Cloneable, CloneableLambda {

    private Traversal.Admin<S, E> traversal;

    public TraversalNextFunction(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
    }

    @Override
    public E apply(final Traverser<S> traverser) {
        final Traverser.Admin<S> split = traverser.asAdmin().split();
        split.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(split);
        return this.traversal.next();
    }

    public Traversal<S, E> getTraversal() {
        return this.traversal;
    }

    @Override
    public String toString() {
        return this.traversal.toString();
    }

    @Override
    public TraversalNextFunction<S, E> clone() throws CloneNotSupportedException {
        final TraversalNextFunction<S, E> clone = (TraversalNextFunction<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }

    @Override
    public TraversalNextFunction<S, E> cloneLambda() throws CloneNotSupportedException {
        return this.clone();
    }
}