package com.tinkerpop.gremlin.pipes;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FlatMapPipe<S, E> extends AbstractPipe<S, E> {

    private Iterator<E> iterator = Collections.emptyIterator();
    private final Function<S, Iterator<E>> function;

    public FlatMapPipe(Function<S, Iterator<E>> function) {
        this.function = function;
    }

    public E processNextStart() {
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next();
            else {
                this.iterator = this.function.apply(this.starts.next());
            }
        }
    }
}
