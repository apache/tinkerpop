package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyTraversal<S, E> implements Traversal.Admin<S, E> {

    private static final EmptyTraversal INSTANCE = new EmptyTraversal();
    private static final TraversalSideEffects SIDE_EFFECTS = EmptyTraversalSideEffects.instance();
    private static final TraversalStrategies STRATEGIES = EmptyTraversalStrategies.instance();

    public static <A, B> EmptyTraversal<A, B> instance() {
        return INSTANCE;
    }

    protected EmptyTraversal() {

    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public E next() {
        throw FastNoSuchElementException.instance();
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return SIDE_EFFECTS;
    }

    @Override
    public void applyStrategies(final TraversalEngine engine) {

    }

    @Override
    public Optional<TraversalEngine> getTraversalEngine() {
        return Optional.empty();
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {

    }

    @Override
    public void addStart(final Traverser<S> start) {

    }

    @Override
    public <E2> Traversal<S, E2> addStep(final Step<?, E2> step) {
        return instance();
    }

    @Override
    public List<Step> getSteps() {
        return Collections.emptyList();
    }

    @Override
    public Traversal<S, E> submit(final GraphComputer computer) {
        return instance();
    }

    @Override
    public EmptyTraversal<S, E> clone() throws CloneNotSupportedException {
        return instance();
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return null;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
    }

    @Override
    public TraversalStrategies getStrategies() {
        return STRATEGIES;
    }

    @Override
    public void setTraversalHolder(final TraversalHolder<?, ?> step) {

    }

    @Override
    public TraversalHolder<?, ?> getTraversalHolder() {
        return (TraversalHolder) EmptyStep.instance();
    }

    @Override
    public void setStrategies(final TraversalStrategies traversalStrategies) {

    }

    @Override
    public <S2, E2> Traversal<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        return (Traversal) this;
    }

    @Override
    public <S2, E2> Traversal<S2, E2> removeStep(final int index) throws IllegalStateException {
        return (Traversal) this;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyTraversal;
    }

    @Override
    public int hashCode() {
        return -343564565;
    }
}
