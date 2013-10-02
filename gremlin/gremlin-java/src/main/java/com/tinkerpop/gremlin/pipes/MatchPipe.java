package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MatchPipe<S, E> extends AbstractPipe<S, E> {

    public MatchPipe(final Pipeline pipeline) {
        super(pipeline);
    }


    public Holder<E> processNextStart() {
        return null;
    }
}
