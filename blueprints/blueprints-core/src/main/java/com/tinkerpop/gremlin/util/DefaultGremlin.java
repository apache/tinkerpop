package com.tinkerpop.gremlin.util;

import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.pipes.map.IdentityPipe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultGremlin<S, E> implements Gremlin<S, E> {

    private final List<Pipe> pipes = new ArrayList<>();

    public DefaultGremlin() {
        this.addPipe(new IdentityPipe(this));
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Pipe<S, ?>) this.pipes.get(0)).addStarts(starts);
    }

    public <S, E> Gremlin<S, E> addPipe(final Pipe<?, E> pipe) {

        if (this.pipes.size() > 0) {
            pipe.setPreviousPipe(this.pipes.get(this.pipes.size() - 1));
            this.pipes.get(this.pipes.size() - 1).setNextPipe(pipe);
        }
        this.pipes.add(pipe);

        return (Gremlin<S, E>) this;
    }

    public boolean hasNext() {
        return this.pipes.get(this.pipes.size() - 1).hasNext();
    }

    public E next() {
        return ((Holder<E>) this.pipes.get(this.pipes.size() - 1).next()).get();
    }

    public String toString() {
        return this.getPipes().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && GremlinHelper.areEqual(this, (Iterator) object);
    }

}
