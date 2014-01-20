package com.tinkerpop.gremlin;

import com.tinkerpop.gremlin.pipes.util.ExpandablePipeIterator;
import com.tinkerpop.gremlin.pipes.util.GremlinHelper;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractPipe<S, E> implements Pipe<S, E> {

    private static final String UNDERSCORE = "_";
    protected String as;
    protected final Pipeline<?, ?> pipeline;
    protected ExpandablePipeIterator<Holder<S>> starts = new ExpandablePipeIterator<>();
    private Holder<E> nextEnd;
    private boolean available;

    public <P extends Pipeline> AbstractPipe(final P pipeline) {
        this.pipeline = pipeline;
        this.as = UNDERSCORE + this.pipeline.getPipes().size();
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        this.starts.add(starts);
    }

    public void setAs(final String as) {
        this.as = as;
    }

    public String getAs() {
        return this.as;
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

    public <P extends Pipeline<?, ?>> P getPipeline() {
        return (P) this.pipeline;
    }

    protected abstract Holder<E> processNextStart() throws NoSuchElementException;

    public String toString() {
        return GremlinHelper.makePipeString(this);
    }
}
