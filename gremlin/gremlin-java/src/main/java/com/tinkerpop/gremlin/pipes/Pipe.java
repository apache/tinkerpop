package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.Holder;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipe<S, E> extends Iterator<Holder<E>>, Serializable {

    public static final Object NO_OBJECT = new Object() {
        public int hashCode() {
            return Integer.MIN_VALUE;
        }

        public boolean equals(final Object object) {
            return object.hashCode() == this.hashCode();
        }
    };

    public void addStarts(final Iterator<Holder<S>> iterator);

    public <P extends Pipeline<?, ?>> P getPipeline();

    public String getAs();

    public void setAs(String as);
}
