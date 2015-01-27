package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalLambda<S, E> implements Function<Traverser<S>, E>, Predicate<Traverser<S>>, Consumer<Traverser<S>>, Cloneable, TraversableLambda<S, E> {

    private Traversal.Admin<S, E> traversal;

    public TraversalLambda(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
    }

    // function
    @Override
    public E apply(final Traverser<S> traverser) {
        final Traverser.Admin<S> split = traverser.asAdmin().split();
        split.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(split);
        try {
            return this.traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided traverser does not map to a value: " + split + "->" + this.traversal);
        }
    }

    // predicate
    @Override
    public boolean test(final Traverser<S> traverser) {
        final Traverser.Admin<S> split = traverser.asAdmin().split();
        split.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(split);
        return this.traversal.hasNext(); // filter
    }

    // consumer
    @Override
    public void accept(final Traverser<S> traverser) {
        final Traverser.Admin<S> split = traverser.asAdmin().split();
        split.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(split);
        this.traversal.iterate(); // sideEffect
    }

    @Override
    public Traversal<S, E> getTraversal() {
        return this.traversal;
    }

    @Override
    public String toString() {
        return this.traversal.toString();
    }

    @Override
    public TraversalLambda<S, E> clone() throws CloneNotSupportedException {
        final TraversalLambda<S, E> clone = (TraversalLambda<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        return clone;
    }

    @Override
    public TraversalLambda<S, E> cloneLambda() throws CloneNotSupportedException {
        return this.clone();
    }

    @Override
    public void reset() {
        this.traversal.reset();
    }
}