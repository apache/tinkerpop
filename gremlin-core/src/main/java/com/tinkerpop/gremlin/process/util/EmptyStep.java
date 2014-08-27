package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyStep<S, E> implements Step<S, E> {

    private static final Step INSTANCE = new EmptyStep<>();

    private EmptyStep() {
    }

    public void addStarts(final Iterator<Traverser<S>> iterator) {

    }

    public void setPreviousStep(final Step<?, S> step) {

    }

    public void reset() {

    }

    public Step<?, S> getPreviousStep() {
        return instance();
    }

    public void setNextStep(final Step<E, ?> step) {

    }

    public Step<E, ?> getNextStep() {
        return instance();
    }

    public <A, B> Traversal<A, B> getTraversal() {
        return EmptyTraversal.instance();
    }

    public Object clone() throws CloneNotSupportedException {
        return instance();
    }

    public String getLabel() {
        return Traverser.NO_FUTURE;
    }

    public void setLabel(String label) {

    }

    public boolean hasNext() {
        return false;
    }

    public E getLast() {
        return (E) NO_OBJECT;
    }

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
