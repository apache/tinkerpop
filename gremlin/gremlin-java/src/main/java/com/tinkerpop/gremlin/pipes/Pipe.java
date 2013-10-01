package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Pipe<S, E> extends Iterator<Holder<E>> {

    public static final Object NO_OBJECT = new Object();

    public Pipe setStarts(final Iterator<Holder<S>> iterator);

    public void addStart(final Holder<S> start);

    public <P extends Pipeline> P getPipeline();
}
