package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipeline;
import com.tinkerpop.gremlin.pipes.Holder;
import com.tinkerpop.gremlin.pipes.Pipe;

import java.util.Iterator;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Optional<Holder<T>> headOptional;
    private final Iterator<T> iterator;
    private final Pipe pipe;

    public <P extends Pipeline> HolderIterator(final Optional<Holder<T>> headOptional, final Pipe pipe, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.headOptional = headOptional;
        this.pipe = pipe;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        if (((Pipeline) this.pipe.getPipeline()).getTrackPaths()) {
            return this.headOptional.isPresent() ?
                    this.headOptional.get().makeChild(this.pipe.getAs(), this.iterator.next()) :
                    new PathHolder<>(this.pipe.getAs(), this.iterator.next());

        } else {
            return this.headOptional.isPresent() ?
                    this.headOptional.get().makeChild(this.pipe.getAs(), this.iterator.next()) :
                    new SimpleHolder<>(this.pipe.getAs(), this.iterator.next());

        }


    }
}
