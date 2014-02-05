package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.Iterator;

public interface Pipe<S, E> extends Iterator<Holder<E>>, Serializable {

    public static final NoObject NO_OBJECT = new NoObject();

    public void addStarts(final Iterator<Holder<S>> iterator);

    public void setPreviousPipe(final Pipe<?, S> pipe);

    public Pipe<?, S> getPreviousPipe();

    public void setNextPipe(final Pipe<E, ?> pipe);

    public Pipe<E, ?> getNextPipe();

    public <S, E> Traversal<S, E> getPipeline();

    public String getAs();

    public void setAs(final String as);

    public static final class NoObject {

        public boolean equals(final Object object) {
            return object instanceof NoObject;
        }
    }
}
