package com.tinkerpop.gremlin.util;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyPipe<S, E> implements Pipe<S, E> {

    private static final Pipe INSTANCE = new EmptyPipe<>();

    public void addStarts(final Iterator<Holder<S>> iterator) {

    }

    public void setPreviousPipe(final Pipe<?, S> pipe) {

    }

    public Pipe<?, S> getPreviousPipe() {
        return instance();
    }

    public void setNextPipe(final Pipe<E, ?> pipe) {

    }

    public Pipe<E, ?> getNextPipe() {
        return instance();
    }

    public <S, E> Pipeline<S, E> getPipeline() {
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
        throw new FastNoSuchElementException();
    }

    public static <S, E> Pipe<S, E> instance() {
        return INSTANCE;
    }
}
