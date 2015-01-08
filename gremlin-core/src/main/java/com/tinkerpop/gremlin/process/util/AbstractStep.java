package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractStep<S, E> implements Step<S, E> {

    protected String label;
    protected Traversal traversal;
    protected ExpandableStepIterator<S> starts;
    protected Traverser<E> nextEnd = null;
    protected boolean futureSetByChild = false;

    protected Step<?, S> previousStep = EmptyStep.instance();
    protected Step<E, ?> nextStep = EmptyStep.instance();
    protected final static boolean PROFILING_ENABLED = "true".equals(System.getProperty(TraversalMetrics.PROFILING_ENABLED));

    public AbstractStep(final Traversal traversal) {
        this.traversal = traversal;
        this.starts = new ExpandableStepIterator<S>((Step) this);
        this.label = Graph.Hidden.hide(Integer.toString(this.traversal.asAdmin().getSteps().size()));
    }

    @Override
    public void reset() {
        this.starts.clear();
        this.nextEnd = null;
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {
        this.starts.add((Iterator) starts);
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.starts.add((Traverser.Admin<S>) start);
    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {
        this.previousStep = step;
    }

    @Override
    public Step<?, S> getPreviousStep() {
        return this.previousStep;
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {
        this.nextStep = step;
    }

    @Override
    public Step<E, ?> getNextStep() {
        return this.nextStep;
    }

    @Override
    public void setLabel(final String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return this.label;
    }

    @Override
    public Traverser<E> next() {
        if (null != this.nextEnd) {
            final Traverser<E> temp = this.prepareTraversalForNextStep(this.nextEnd);
            this.nextEnd = null;
            return temp;
        } else {
            while (true) {
                final Traverser<E> traverser = this.processNextStart();
                if (0 != traverser.bulk()) {
                    return this.prepareTraversalForNextStep(traverser);
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (null != this.nextEnd)
            return true;
        else {
            try {
                while (true) {
                    this.nextEnd = this.processNextStart();
                    if (0 != this.nextEnd.bulk())
                        return true;
                    else
                        this.nextEnd = null;
                }
            } catch (final NoSuchElementException e) {
                return false;
            }
        }
    }

    @Override
    public <A, B> Traversal<A, B> getTraversal() {
        return this.traversal;
    }

    @Override
    public void setTraversal(final Traversal<?, ?> traversal) {
        this.traversal = traversal;
    }

    protected abstract Traverser<E> processNextStart() throws NoSuchElementException;

    public String toString() {
        return TraversalHelper.makeStepString(this);
    }

    @Override
    public AbstractStep<S, E> clone() throws CloneNotSupportedException {
        final AbstractStep clone = (AbstractStep) super.clone();
        clone.starts = new ExpandableStepIterator<S>(clone);
        clone.previousStep = EmptyStep.instance();
        clone.nextStep = EmptyStep.instance();
        clone.nextEnd = null;
        return clone;
    }

    private final Traverser<E> prepareTraversalForNextStep(final Traverser<E> traverser) {
        if (!this.futureSetByChild) ((Traverser.Admin<E>) traverser).setFuture(this.nextStep.getLabel());
        traverser.path().addLabel(this.getLabel());
        return traverser;
    }

}
