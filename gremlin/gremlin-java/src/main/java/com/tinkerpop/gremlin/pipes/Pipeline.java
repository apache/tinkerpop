package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipeline<S, E> extends Pipe<S, E> {

    public <R extends Pipeline> R addPipe(final Pipe pipe);

    public Pipe<Object, E> lastPipe();

    public default boolean hasNext() {
        return this.lastPipe().hasNext();
    }

    public default Holder<E> next() {
        return this.lastPipe().next();
    }

    public default Holder<E> getCurrentEnd() {
        return this.lastPipe().getCurrentEnd();
    }

}
