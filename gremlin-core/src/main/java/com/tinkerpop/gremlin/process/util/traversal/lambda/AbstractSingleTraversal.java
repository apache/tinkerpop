package com.tinkerpop.gremlin.process.util.traversal.lambda;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.traverser.O_TraverserGenerator;
import com.tinkerpop.gremlin.process.util.step.EmptyStep;
import com.tinkerpop.gremlin.process.util.traversal.util.EmptyTraversalSideEffects;
import com.tinkerpop.gremlin.process.util.traversal.util.EmptyTraversalStrategies;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractSingleTraversal<S, E> implements Traversal.Admin<S, E> {

    @Override
    public List<Step> getSteps() {
        return Collections.emptyList();
    }

    @Override
    public void reset() {

    }

    @Override
    public <S2, E2> Traversal<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        return (Traversal<S2, E2>) this;
    }

    @Override
    public <S2, E2> Traversal<S2, E2> removeStep(final int index) throws IllegalStateException {
        return (Traversal<S2, E2>) this;
    }

    @Override
    public void applyStrategies(final TraversalEngine engine) throws IllegalStateException {

    }

    @Override
    public Optional<TraversalEngine> getTraversalEngine() {
        return Optional.of(TraversalEngine.STANDARD);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return O_TraverserGenerator.instance();
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return EmptyTraversalSideEffects.instance();
    }

    @Override
    public void setStrategies(final TraversalStrategies strategies) {

    }

    @Override
    public TraversalStrategies getStrategies() {
        return EmptyTraversalStrategies.instance();
    }

    @Override
    public void setTraversalHolder(final TraversalHolder step) {

    }

    @Override
    public TraversalHolder getTraversalHolder() {
        return (TraversalHolder) EmptyStep.instance();
    }

    @Override
    public Traversal.Admin<S, E> clone() throws CloneNotSupportedException {
        return (AbstractSingleTraversal<S, E>) super.clone();
    }

    @Override
    public E next() {
        throw new UnsupportedOperationException("The " + this.getClass().getSimpleName() + " can only be used as a predicate traversal");
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    public void addStart(final Traverser<S> start) {
    }

}
