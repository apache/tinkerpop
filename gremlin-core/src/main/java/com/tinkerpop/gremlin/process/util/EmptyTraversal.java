package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.GraphComputer;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyTraversal<S, E> implements Traversal<S, E> {

    private static final EmptyTraversal INSTANCE = new EmptyTraversal();
    private static final SideEffects SIDE_EFFECTS = new DefaultTraversalSideEffects();

    public static <A, B> EmptyTraversal<A, B> instance() {
        return INSTANCE;
    }

    protected EmptyTraversal() {

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
    public SideEffects sideEffects() {
        return SIDE_EFFECTS;
    }

    @Override
    public void applyStrategies(final TraversalEngine engine) {

    }

    @Override
    public boolean isLocked() {
        return true;
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
    public EmptyTraversal<S, E> clone() {
        return instance();
    }
}
