package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ObjectTraversalHasNextPredicate<S, E> implements Predicate<S>, Cloneable, CloneableLambda {

    private Traversal.Admin<S, E> traversal;

    public ObjectTraversalHasNextPredicate(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
    }

    @Override
    public boolean test(final S start) {
        final Traverser.Admin<S> traverser = this.traversal.getTraverserGenerator().generate(start, this.traversal.getStartStep(), 1l);
        traverser.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(traverser);
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
    public ObjectTraversalHasNextPredicate<S, E> clone() throws CloneNotSupportedException {
        final ObjectTraversalHasNextPredicate<S, E> clone = (ObjectTraversalHasNextPredicate<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }

    @Override
    public ObjectTraversalHasNextPredicate<S, E> cloneLambda() throws CloneNotSupportedException {
        return this.clone();
    }
}