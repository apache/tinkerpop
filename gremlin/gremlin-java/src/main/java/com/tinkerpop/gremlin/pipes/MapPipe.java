package com.tinkerpop.gremlin.pipes;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapPipe<S, E> extends AbstractPipe<S, E> {

    private final Function<S, E> function;

    public MapPipe(Function<S, E> function) {
        this.function = function;
    }

    public E processNextStart() {
        return this.function.apply(this.starts.next());
    }
}
