package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
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

    protected final List<Step> steps = new ArrayList<>();
    protected final Strategies strategies = new DefaultStrategies(this);
    protected final SideEffects sideEffects = new DefaultSideEffects(this);

    public DefaultTraversal() {
        this.strategies.register(TraverserSourceStrategy.instance());
        this.strategies.register(LabeledEndStepStrategy.instance());
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
        final List<Step> temp = new ArrayList<>();
        Step currentStep = TraversalHelper.getStart(this);
        while (!(currentStep instanceof EmptyStep)) {
            temp.add(currentStep);
            currentStep = currentStep.getNextStep();
        }
        return temp.toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    private void applyStrategies() {
        if (!this.strategies.complete()) this.strategies.apply();
    }
}
