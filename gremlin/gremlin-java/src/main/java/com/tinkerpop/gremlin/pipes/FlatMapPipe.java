package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.HolderIterator;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapPipe<S, E> extends AbstractPipe<S, E> {

    private Iterator<Holder<E>> iterator = Collections.emptyIterator();
    private final Function<Holder<S>, Iterator<E>> function;

    public FlatMapPipe(Function<Holder<S>, Iterator<E>> function) {
        this.function = function;
    }

    public Holder<E> processNextStart() {
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next();
            else {
                final Holder<S> h = this.starts.next();
                this.iterator = new HolderIterator<>(h, this.function.apply(h));
            }
        }
    }
}
