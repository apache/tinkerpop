package com.tinkerpop.gremlin.process;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGenerator {

    public <S> Traverser.Admin<S> generate(final S start, final Step<?, S> startStep);

    public default <S> Iterator<Traverser.Admin<S>> generateIterator(final Iterator<S> starts, final Step<?, S> startStep) {
        return new Iterator<Traverser.Admin<S>>() {
            @Override
            public boolean hasNext() {
                return starts.hasNext();
            }

            @Override
            public Traverser.Admin<S> next() {
                return generate(starts.next(), startStep);
            }
        };
    }
}
