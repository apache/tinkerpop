package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.PathTraverser;
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
    public ExpandableStepIterator<S> starts;
    protected Traverser<E> nextEnd = null;
    protected boolean available = false;
    protected boolean futureSetByChild = false;

    protected Step<?, S> previousStep = EmptyStep.instance();
    protected Step<E, ?> nextStep = EmptyStep.instance();
    protected final static boolean PROFILING_ENABLED = "true".equals(System.getProperty(TraversalMetrics.PROFILING_ENABLED));
    
    public AbstractStep(final Traversal traversal) {
        this.traversal = traversal;
        this.starts = new ExpandableStepIterator<S>((Step) this);
        this.label = Graph.System.system(Integer.toString(this.traversal.getSteps().size()));
    }

    @Override
    public void reset() {
        this.starts.clear();
        this.available = false;
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
        if (this.available) {
            this.available = false;
            prepareTraversalForNextStep(this.nextEnd);
            return this.nextEnd;
        } else {
            while (true) {
                final Traverser<E> traverser = this.processNextStart();
                if (traverser.bulk() != 0) {
                    prepareTraversalForNextStep(traverser);
                    return traverser;
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (this.available)
            return true;
        else {
            try {
                while (true) {
                    this.nextEnd = this.processNextStart();
                    if (this.nextEnd.bulk() != 0) {
                        this.available = true;
                        return true;
                    }
                }
            } catch (final NoSuchElementException e) {
                this.available = false;
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
        final AbstractStep step = (AbstractStep) super.clone();
        step.starts = new ExpandableStepIterator<S>(step);
        step.previousStep = EmptyStep.instance();
        step.nextStep = EmptyStep.instance();
        step.available = false;
        step.nextEnd = null;
        return step;
    }

    private void prepareTraversalForNextStep(final Traverser<E> traverser) {
        if (!this.futureSetByChild)
            ((Traverser.Admin<E>) traverser).setFuture(this.nextStep.getLabel());
        if (traverser instanceof PathTraverser) traverser.path().addLabel(this.getLabel());
        if (TraversalHelper.isLabeled(this.label))
            this.traversal.sideEffects().set(this.label, traverser.get());
    }

}
