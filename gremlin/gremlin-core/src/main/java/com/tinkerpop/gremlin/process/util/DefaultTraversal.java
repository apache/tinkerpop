package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Memory;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.oltp.map.IdentityPipe;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    private final List<Pipe> pipes = new ArrayList<>();

    public DefaultTraversal() {
        this.addPipe(new IdentityPipe(this));
    }

    public List<Pipe> getPipes() {
        return this.pipes;
    }

    public Memory memory() {
        return null;
    }

    public Optimizers optimizers() {
        return null;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Pipe<S, ?>) this.pipes.get(0)).addStarts(starts);
    }

    public <S, E> Traversal<S, E> addPipe(final Pipe<?, E> pipe) {

        if (this.pipes.size() > 0) {
            pipe.setPreviousPipe(this.pipes.get(this.pipes.size() - 1));
            this.pipes.get(this.pipes.size() - 1).setNextPipe(pipe);
        }
        this.pipes.add(pipe);

        return (Traversal<S, E>) this;
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
