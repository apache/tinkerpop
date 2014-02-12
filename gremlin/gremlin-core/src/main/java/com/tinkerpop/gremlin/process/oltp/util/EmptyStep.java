package com.tinkerpop.gremlin.process.oltp.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyStep<S, E> implements Step<S, E> {

    private static final Step INSTANCE = new EmptyStep<>();

    private EmptyStep() {
    }

    public void addStarts(final Iterator<Holder<S>> iterator) {

    }

    public void setPreviousStep(final Step<?, S> step) {

    }

    public Step<?, S> getPreviousStep() {
        return instance();
    }

    public void setNextStep(final Step<E, ?> step) {

    }

    public Step<E, ?> getNextStep() {
        return instance();
    }

    public <S, E> Traversal<S, E> getPipeline() {
        return null;
    }

    public String getAs() {
        return Holder.NO_FUTURE;
    }

    public void setAs(String as) {

    }

    public boolean hasNext() {
        return false;
    }

    public Holder<E> next() {
        throw FastNoSuchElementException.instance();
    }

    public static <S, E> Step<S, E> instance() {
        return INSTANCE;
    }
}
