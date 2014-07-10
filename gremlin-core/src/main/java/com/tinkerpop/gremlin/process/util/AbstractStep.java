package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractStep<S, E> implements Step<S, E> {

    private static final String UNDERSCORE = "_";
    protected String as;
    protected final Traversal traversal;
    protected ExpandableStepIterator<S> starts;
    protected Traverser<E> nextEnd;
    protected boolean available;

    protected Step<?, S> previousStep = EmptyStep.instance();
    protected Step<E, ?> nextStep = EmptyStep.instance();

    public AbstractStep(final Traversal traversal) {
        this.traversal = traversal;
        this.starts = new ExpandableStepIterator<S>((Step) this);
        this.as = UNDERSCORE + this.traversal.getSteps().size();
    }

    public void addStarts(final Iterator<Traverser<S>> starts) {
        this.starts.add((Iterator) starts);
    }

    public void setPreviousStep(final Step<?, S> step) {
        this.previousStep = step;
    }

    public Step<?, S> getPreviousStep() {
        return this.previousStep;
    }

    public void setNextStep(final Step<E, ?> step) {
        this.nextStep = step;
    }

    public Step<E, ?> getNextStep() {
        return this.nextStep;
    }

    public void setAs(final String as) {
        this.as = as;
    }

    public String getAs() {
        return this.as;
    }

    public Traverser<E> next() {
        if (this.available) {
            this.available = false;
            return this.nextEnd;
        } else {
            final Traverser<E> traverser = this.processNextStart();
            traverser.setFuture(this.nextStep.getAs());
            return traverser;
        }
    }

    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                this.nextEnd = this.processNextStart();
                this.nextEnd.setFuture(this.nextStep.getAs());
                this.available = true;
                return true;
            } catch (final NoSuchElementException e) {
                this.available = false;
                return false;
            }
        }
    }

    public <S, E> Traversal<S, E> getTraversal() {
        return this.traversal;
    }

    protected abstract Traverser<E> processNextStart() throws NoSuchElementException;

    public String toString() {
        return TraversalHelper.makeStepString(this);
    }
}
