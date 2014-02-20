package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Memory;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;

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
    private static final Optimizers OPTIMIZERS = new DefaultOptimizers();   // TODO: make "empty optimizers?"

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

    public Optimizers optimizers() {
        return OPTIMIZERS;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {

    }

    public <S, E> Traversal<S, E> addStep(final Step<?, E> step) {
        return (Traversal) this;
    }

    public List<Step> getSteps() {
        return Collections.EMPTY_LIST;
    }

    public Iterator<E> submit(final TraversalEngine engine) {
        return Collections.emptyIterator();
    }
}
