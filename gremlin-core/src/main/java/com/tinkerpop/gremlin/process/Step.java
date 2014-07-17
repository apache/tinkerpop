package com.tinkerpop.gremlin.process;

import java.io.Serializable;
import java.util.Iterator;

public interface Step<S, E> extends Iterator<Traverser<E>>, Serializable {

    public static final NoObject NO_OBJECT = new NoObject();

    public void addStarts(final Iterator<Traverser<S>> iterator);

    public void setPreviousStep(final Step<?, S> step);

    public Step<?, S> getPreviousStep();

    public void setNextStep(final Step<E, ?> step);

    public Step<E, ?> getNextStep();

    public <S, E> Traversal<S, E> getTraversal();

    public void dehydrateStep();

    public String getAs();

    public void setAs(final String as);

    public static final class NoObject {

        private NoObject() {
        }

        public boolean equals(final Object object) {
            return object instanceof NoObject;
        }
    }
}
