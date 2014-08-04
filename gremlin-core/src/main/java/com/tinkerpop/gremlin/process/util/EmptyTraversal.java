package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
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
    private static final Memory MEMORY = new DefaultMemory();         // TODO: make "empty memory?"
    private static final TraversalStrategies TRAVERSAL_STRATEGIES = new DefaultTraversalStrategies(new EmptyTraversal<>());

    public static EmptyTraversal instance() {
        return INSTANCE;
    }

    public boolean hasNext() {
        return false;
    }

    public E next() {
        throw FastNoSuchElementException.instance();
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

    public Traversal<S, E> submit(final GraphComputer computer) {
        return new EmptyTraversal<>();
    }
}
