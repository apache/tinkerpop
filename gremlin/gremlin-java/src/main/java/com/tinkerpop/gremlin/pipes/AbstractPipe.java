package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.ExpandableIterator;
import com.tinkerpop.gremlin.pipes.util.Holder;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractPipe<S, E> implements Pipe<S, E> {

    protected String name = null;
    protected final Pipeline pipeline;
    protected ExpandableIterator<Holder<S>> starts = new ExpandableIterator();
    private Holder<E> nextEnd;
    protected Holder<E> currentEnd;
    protected boolean available;

    public <P extends Pipeline> AbstractPipe(P pipeline) {
        this.pipeline = pipeline;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        this.starts.add(starts);
    }

    public void setName(final String name) {
       // if (null != name)
        //    throw new IllegalStateException("Pipe has already been named");
        //else
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public Holder<E> next() {
        if (this.available) {
            this.available = false;
            return (this.currentEnd = this.nextEnd);
        } else {
            return (this.currentEnd = this.processNextStart());
        }
    }

    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                this.nextEnd = this.processNextStart();
                return (this.available = true);
            } catch (final NoSuchElementException e) {
                return (this.available = false);
            }
        }
    }

    public <P extends Pipeline> P getPipeline() {
        return (P) this.pipeline;
    }

    protected abstract Holder<E> processNextStart() throws NoSuchElementException;


}
