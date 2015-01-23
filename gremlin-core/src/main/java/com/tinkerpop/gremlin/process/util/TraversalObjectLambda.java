package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.util.function.CloneableLambda;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalObjectLambda<S, E> implements Function<S, E>, Predicate<S>, Consumer<S>, Cloneable, CloneableLambda {

    private Traversal.Admin<S, E> traversal;
    private TraverserGenerator generator;

    public TraversalObjectLambda(final Traversal<S, E> traversal) {
        this.traversal = traversal.asAdmin();
        this.generator = this.traversal.getTraverserGenerator();
    }

    // function
    @Override
    public E apply(final S start) {
        final Traverser.Admin<S> traverser = this.generator.generate(start, this.traversal.getStartStep(), 1l);
        traverser.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(traverser);
        return this.traversal.next();
    }

    // predicate
    @Override
    public boolean test(final S start) {
        final Traverser.Admin<S> traverser = this.generator.generate(start, this.traversal.getStartStep(), 1l);
        traverser.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(traverser);
        return this.traversal.hasNext();
    }

    // consumer
    @Override
    public void accept(final S start) {
        final Traverser.Admin<S> traverser = this.generator.generate(start, this.traversal.getStartStep(), 1l);
        traverser.setSideEffects(this.traversal.getSideEffects());
        this.traversal.reset();
        this.traversal.addStart(traverser);
        this.traversal.iterate();
    }

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

    @Override
    public TraversalObjectLambda<S, E> cloneLambda() throws CloneNotSupportedException {
        return this.clone();
    }


}