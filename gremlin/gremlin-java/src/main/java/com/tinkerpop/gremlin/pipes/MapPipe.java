package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapPipe<S, E> extends AbstractPipe<S, E> {

    private final Function<Holder<S>, E> function;

    public MapPipe(final Function<Holder<S>, E> function) {
        this.function = function;
    }

    public Holder<E> processNextStart() {
        final Holder<S> h = this.starts.next();
        return new Holder<>(this.function.apply(h), h);
    }
}
