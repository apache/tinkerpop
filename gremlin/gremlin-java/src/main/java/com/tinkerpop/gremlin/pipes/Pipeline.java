package com.tinkerpop.gremlin.pipes;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipeline<S, E> extends Pipe<S, E> {

    public <T> Gremlin<S, T> addPipe(final Pipe pipe);

    public Pipe<?, E> lastPipe();

    public default boolean hasNext() {
        return this.lastPipe().hasNext();
    }

    public default E next() {
        return this.lastPipe().next();
    }

    public default E getCurrentEnd() {
        return this.lastPipe().getCurrentEnd();
    }

}
