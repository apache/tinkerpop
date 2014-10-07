package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyStep<S, E> implements Step<S, E> {

    private static final EmptyStep INSTANCE = new EmptyStep<>();

    private EmptyStep() {
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> iterator) {

    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {

    }

    @Override
    public void reset() {

    }

    @Override
    public Step<?, S> getPreviousStep() {
        return instance();
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {

    }

    @Override
    public Step<E, ?> getNextStep() {
        return instance();
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
        return (EmptyStep<S, E>) instance();
    }

    @Override
    public String getLabel() {
        return Traverser.Admin.NO_FUTURE;
    }

    @Override
    public void setLabel(String label) {

    }

    @Override
    public boolean hasNext() {
        return false;
    }

    public E getLast() {
        return (E) NO_OBJECT;
    }

    @Override
    public Traverser<E> next() {
        throw FastNoSuchElementException.instance();
    }

    public static <S, E> Step<S, E> instance() {
        return INSTANCE;
    }

    public boolean equals(final Object object) {
        return object instanceof EmptyStep;
    }
}
