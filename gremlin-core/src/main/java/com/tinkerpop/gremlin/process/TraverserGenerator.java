package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraverserGenerator {

    public Set<TraverserRequirement> requirements();

    public <S> Traverser.Admin<S> generate(final S start, final Step<S, ?> startStep, final long initialBulk);

    public default <S> Iterator<Traverser.Admin<S>> generateIterator(final Iterator<S> starts, final Step<S, ?> startStep, final long initialBulk) {
        return new Iterator<Traverser.Admin<S>>() {
            @Override
            public boolean hasNext() {
                return starts.hasNext();
            }

            @Override
            public Traverser.Admin<S> next() {
                return generate(starts.next(), startStep, initialBulk);
            }
        };
    }
}
