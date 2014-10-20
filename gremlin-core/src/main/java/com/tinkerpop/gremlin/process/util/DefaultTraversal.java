package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    private E lastEnd = null;
    private long lastEndCount = 0l;


    protected List<Step> steps = new ArrayList<>();
    protected DefaultStrategies strategies = new DefaultStrategies(this);
    protected DefaultSideEffects sideEffects = new DefaultSideEffects();

    public DefaultTraversal() {

    }

    public DefaultTraversal(final Graph graph) {
        this.sideEffects().setGraph(graph);
    }

    @Override
    public List<Step> getSteps() {
        return this.steps;
    }

    @Override
    public SideEffects sideEffects() {
        return this.sideEffects;
    }

    @Override
    public Strategies strategies() {
        return this.strategies;
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {
        TraversalHelper.getStart(this).addStarts(starts);
    }

    @Override
    public void addStart(final Traverser<S> start) {
        TraversalHelper.getStart(this).addStart(start);
    }

    @Override
    public boolean hasNext() {
        this.applyStrategies();
        return this.lastEndCount > 0l || TraversalHelper.getEnd(this).hasNext();
    }

    @Override
    public E next() {
        this.applyStrategies();
        if (this.lastEndCount > 0l) {
            this.lastEndCount--;
            return this.lastEnd;
        } else {
            final Traverser<E> next = TraversalHelper.getEnd(this).next();
            if (next.bulk() == 1) {
                return next.get();
            } else {
                this.lastEndCount = next.bulk() - 1;
                this.lastEnd = next.get();
                return this.lastEnd;
            }
        }
    }

    public String toString() {
        return TraversalHelper.makeTraversalString(this);
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    private final void applyStrategies() {
        if (!this.strategies.complete()) this.strategies.apply();
    }

    @Override
    public DefaultTraversal<S, E> clone() {
        try {
            final DefaultTraversal<S, E> clone = (DefaultTraversal<S, E>) super.clone();
            clone.steps = new ArrayList<>();
            for (int i = this.steps.size() - 1; i >= 0; i--) {
                final Step<?, ?> clonedStep = this.steps.get(i).clone();
                clonedStep.setTraversal(clone);
                TraversalHelper.insertStep(clonedStep, 0, clone);
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
