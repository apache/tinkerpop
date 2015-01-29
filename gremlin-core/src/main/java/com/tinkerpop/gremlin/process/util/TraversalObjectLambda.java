package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.util.function.TraversableLambda;

import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalObjectLambda<S, E> implements Function<S, E>, Predicate<S>, Consumer<S>, Cloneable, TraversableLambda<S,E> {

    private Traversal.Admin<S, E> traversal;
    private TraverserGenerator generator;

    public TraversalObjectLambda(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
        this.generator = this.traversal.getTraverserGenerator();
    }

    // function
    @Override
    public E apply(final S start) {
        this.traversal.reset();
        this.traversal.addStart(this.generator.generate(start, this.traversal.getStartStep(), 1l));
        try {
            return this.traversal.next(); // map
        } catch (final NoSuchElementException e) {
            throw new IllegalArgumentException("The provided start does not map to a value: " + start + "->" + this.traversal);
        }
    }

    // predicate
    @Override
    public boolean test(final S start) {
        this.traversal.reset();
        this.traversal.addStart(this.generator.generate(start, this.traversal.getStartStep(), 1l));
        return this.traversal.hasNext();
    }

    // consumer
    @Override
    public void accept(final S start) {
        this.traversal.reset();
        this.traversal.addStart(this.generator.generate(start, this.traversal.getStartStep(), 1l));
        this.traversal.iterate();
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
    public TraversalObjectLambda<S, E> clone() throws CloneNotSupportedException {
        final TraversalObjectLambda<S, E> clone = (TraversalObjectLambda<S, E>) super.clone();
        clone.traversal = this.traversal.clone().asAdmin();
        clone.generator = clone.traversal.getTraverserGenerator();
        return clone;
    }
}