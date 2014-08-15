package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.strategy.AsStrategy;
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
    protected final Memory memory = new DefaultMemory();

    public DefaultTraversal() {
        this.strategies.register(TraverserSourceStrategy.instance());
        this.strategies.register(AsStrategy.instance());
    }

    public DefaultTraversal(final Graph graph) {
        this();
        this.memory().setGraph(graph);
    }

    public List<Step> getSteps() {
        return this.steps;
    }

    public Memory memory() {
        return this.memory;
    }

    public Strategies strategies() {
        return this.strategies;
    }

    public void addStarts(final Iterator<Traverser<S>> starts) {
        ((Step<S, ?>) this.steps.get(0)).addStarts(starts);
    }

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step) {
        TraversalHelper.insertStep(step, this.getSteps().size(), this);
        return (T) this;
    }

    public boolean hasNext() {
        this.applyStrategies();
        return this.steps.get(this.steps.size() - 1).hasNext();
    }

    public E next() {
        this.applyStrategies();
        return ((Traverser<E>) this.steps.get(this.steps.size() - 1).next()).get();
    }

    public String toString() {
        return this.getSteps().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    private void applyStrategies() {
        if (!this.strategies.complete()) this.strategies.apply();
    }
}
