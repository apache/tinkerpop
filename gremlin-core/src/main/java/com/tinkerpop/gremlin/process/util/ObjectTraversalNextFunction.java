package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ObjectTraversalNextFunction<S, E> implements Function<S, E>, Cloneable, CloneableLambda {

    private Traversal.Admin<S, E> traversal;

    public ObjectTraversalNextFunction(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
    }

    @Override
    public E apply(final S start) {
        final Traverser.Admin<S> traverser = this.traversal.getTraverserGenerator().generate(start,this.traversal.getStartStep(),1l);
        traverser.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(traverser);
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
    public ObjectTraversalNextFunction<S, E> clone() throws CloneNotSupportedException {
        final ObjectTraversalNextFunction<S, E> clone = (ObjectTraversalNextFunction<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }

    @Override
    public ObjectTraversalNextFunction<S, E> cloneLambda() throws CloneNotSupportedException {
        return this.clone();
    }
}