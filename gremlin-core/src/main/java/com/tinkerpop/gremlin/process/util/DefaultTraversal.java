package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.strategy.GraphStandardStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.LabeledEndStepStrategy;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    protected List<Step> steps = new ArrayList<>();
    protected DefaultStrategies strategies = new DefaultStrategies(this);
    protected DefaultSideEffects sideEffects = new DefaultSideEffects();

    public DefaultTraversal() {
        this.strategies.register(TraverserSourceStrategy.instance());
        this.strategies.register(LabeledEndStepStrategy.instance());
        this.strategies().register(GraphStandardStrategy.instance());
    }

    public DefaultTraversal(final Graph graph) {
        this();
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
    public boolean hasNext() {
        this.applyStrategies();
        return TraversalHelper.getEnd(this).hasNext();
    }

    @Override
    public E next() {
        this.applyStrategies();
        return TraversalHelper.getEnd(this).next().get();
    }

    public String toString() {
        return TraversalHelper.makeTraversalString(this);
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    private void applyStrategies() {
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
