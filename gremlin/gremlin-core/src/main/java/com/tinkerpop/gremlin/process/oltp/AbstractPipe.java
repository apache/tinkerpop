package com.tinkerpop.gremlin.process.oltp;

import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.util.EmptyPipe;
import com.tinkerpop.gremlin.util.ExpandablePipeIterator;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractPipe<S, E> implements Pipe<S, E> {

    private static final String UNDERSCORE = "_";
    protected String as;
    protected final Traversal pipeline;
    protected ExpandablePipeIterator<S> starts;
    private Holder<E> nextEnd;
    private boolean available;

    protected Pipe<?, S> previousPipe = EmptyPipe.instance();
    protected Pipe<E, ?> nextPipe = EmptyPipe.instance();

    public AbstractPipe(final Traversal pipeline) {
        this.pipeline = pipeline;
        this.starts = new ExpandablePipeIterator((Pipe) this);
        this.as = UNDERSCORE + this.pipeline.getPipes().size();
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        this.starts.add((Iterator) starts);
    }

    public void setPreviousPipe(final Pipe<?, S> pipe) {
        this.previousPipe = pipe;
    }

    public Pipe<?, S> getPreviousPipe() {
        return this.previousPipe;
    }

    public void setNextPipe(final Pipe<E, ?> pipe) {
        this.nextPipe = pipe;
    }

    public Pipe<E, ?> getNextPipe() {
        return this.nextPipe;
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
            final Holder<E> holder = this.processNextStart();
            holder.setFuture(this.nextPipe.getAs());
            return holder;
        }
    }

    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                this.nextEnd = this.processNextStart();
                this.nextEnd.setFuture(this.nextPipe.getAs());
                this.available = true;
                return true;
            } catch (final NoSuchElementException e) {
                this.available = false;
                return false;
            }
        }
    }

    public <S, E> Traversal<S, E> getPipeline() {
        return this.pipeline;
    }

    protected abstract Holder<E> processNextStart() throws NoSuchElementException;

    public String toString() {
        return GremlinHelper.makePipeString(this);
    }
}
