package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.ExpandablePipeIterator;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractPipe<S, E> implements Pipe<S, E> {

    protected String name = Pipe.NONE;
    protected final Pipeline pipeline;
    protected ExpandablePipeIterator<Holder<S>> starts = new ExpandablePipeIterator<>();
    private Holder<E> nextEnd;
    private boolean available;

    public <P extends Pipeline> AbstractPipe(final P pipeline) {
        this.pipeline = pipeline;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        this.starts.add(starts);
    }

    public void setName(final String name) {
        if (name.equals(Pipe.NONE))
            throw new IllegalArgumentException("The name 'none' is reserved to denote no name");
        if (!this.name.equals(Pipe.NONE))
            throw new IllegalStateException("Pipe has already been named " + this.name);
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public Holder<E> next() {
        if (this.available) {
            this.available = false;
            return this.nextEnd;
        } else {
            return this.processNextStart();
        }
    }

    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                this.nextEnd = this.processNextStart();
                this.available = true;
                return true;
            } catch (final NoSuchElementException e) {
                this.available = false;
                return false;
            }
        }
    }

    public <P extends Pipeline> P getPipeline() {
        return (P) this.pipeline;
    }

    protected abstract Holder<E> processNextStart() throws NoSuchElementException;


}
