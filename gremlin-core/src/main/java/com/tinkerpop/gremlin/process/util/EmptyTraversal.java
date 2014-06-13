package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.strategy.DefaultTraversalStrategies;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyTraversal<S, E> implements Traversal<S, E> {

    private static final EmptyTraversal INSTANCE = new EmptyTraversal();
    private static final Memory MEMORY = new DefaultMemory();         // TODO: make "empty memory?"
    private static final TraversalStrategies TRAVERSAL_STRATEGIES = new DefaultTraversalStrategies();   // TODO: make "empty traversalStrategies?"

    public static EmptyTraversal instance() {
        return INSTANCE;
    }

    public boolean hasNext() {
        return false;
    }

    public E next() {
        throw new NoSuchElementException();
    }

    public Memory memory() {
        return MEMORY;
    }

    public TraversalStrategies strategies() {
        return TRAVERSAL_STRATEGIES;
    }

    public void addStarts(final Iterator<Traverser<S>> starts) {

    }

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step) {
        return (T) this;
    }

    public List<Step> getSteps() {
        return Collections.EMPTY_LIST;
    }

    public Traversal<S, E> submit(final TraversalEngine engine) {
        return new EmptyTraversal<>();
    }
}
