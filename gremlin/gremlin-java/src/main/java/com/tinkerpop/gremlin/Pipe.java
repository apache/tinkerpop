package com.tinkerpop.gremlin;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipe<S, E> extends Iterator<Holder<E>>, Serializable {

    public static final NoObject NO_OBJECT = new NoObject();

    public void addStarts(final Iterator<Holder<S>> iterator);

    public <P extends Pipeline<?, ?>> P getPipeline();

    public String getAs();

    public void setAs(String as);

    public static final class NoObject {

        public boolean equals(final Object object) {
            return object instanceof NoObject;
        }
    }
}
