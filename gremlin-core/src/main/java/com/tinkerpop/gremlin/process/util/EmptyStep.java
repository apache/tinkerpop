package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyStep<S, E> implements Step<S, E>, TraversalHolder<S, E> {

    private static final EmptyStep INSTANCE = new EmptyStep<>();

    public static <S, E> Step<S, E> instance() {
        return INSTANCE;
    }

    private EmptyStep() {
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {

    }

    @Override
    public void addStart(final Traverser<S> start) {

    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {

    }

    @Override
    public void reset() {

    }

    @Override
    public Step<?, S> getPreviousStep() {
        return INSTANCE;
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {

    }

    @Override
    public Step<E, ?> getNextStep() {
        return INSTANCE;
    }

    @Override
    public <A, B> Traversal<A, B> getTraversal() {
        return EmptyTraversal.instance();
    }

    @Override
    public void setTraversal(final Traversal<?, ?> traversal) {

    }

    @Override
    public EmptyStep<S, E> clone() throws CloneNotSupportedException {
        return INSTANCE;
    }

    @Override
    public Optional<String> getLabel() {
        return Optional.empty();
    }

    @Override
    public void setLabel(String label) {

    }

    @Override
    public void setId(String id) {

    }

    @Override
    public String getId() {
        return Traverser.Admin.HALT;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Traverser<E> next() {
        throw FastNoSuchElementException.instance();
    }

    @Override
    public int hashCode() {
        return -1691648095;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyStep;
    }

    @Override
    public List<Traversal<S, E>> getTraversals() {
        return Collections.emptyList();
    }
}
