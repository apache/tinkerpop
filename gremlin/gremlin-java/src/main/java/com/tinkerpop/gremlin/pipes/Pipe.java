package com.tinkerpop.gremlin.pipes;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipe<S, E> extends Iterator<E> {

    public Pipe setStarts(final Iterator<S> iterator);

    public void addStart(final S start);

    public E getCurrentEnd();
}
