package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.strategy.DefaultTraversalStrategies;
import com.tinkerpop.gremlin.process.graph.strategy.TraverserSourceStrategy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    protected final List<Step> steps = new ArrayList<>();
    protected final TraversalStrategies traversalStrategies = new DefaultTraversalStrategies();
    protected final Memory memory = new DefaultMemory();
    protected boolean firstNext = true;

    public DefaultTraversal() {
        this.traversalStrategies.register(new TraverserSourceStrategy());
    }

    public List<Step> getSteps() {
        return this.steps;
    }

    public Memory memory() {
        return this.memory;
    }

    public TraversalStrategies strategies() {
        return traversalStrategies;
    }

    public void addStarts(final Iterator<Traverser<S>> starts) {
        ((Step<S, ?>) this.steps.get(0)).addStarts(starts);
    }

    public <S, E, T extends Traversal<S, E>> T addStep(final Step<?, E> step) {
        if (this.steps.size() > 0) {
            step.setPreviousStep(this.steps.get(this.steps.size() - 1));
            this.steps.get(this.steps.size() - 1).setNextStep(step);
        }
        this.steps.add(step);

        return (T) this;
    }

    public boolean hasNext() {
        this.doFinalOptimization();
        return this.steps.get(this.steps.size() - 1).hasNext();
    }

    public E next() {
        this.doFinalOptimization();
        return ((Traverser<E>) this.steps.get(this.steps.size() - 1).next()).get();
    }

    public String toString() {
        // todo: optimizing on toString can cause weird stuff when debugging - can we have doPreflightFinalOptimization?
        //this.doFinalOptimization();
        return this.getSteps().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    private final void doFinalOptimization() {
        if (this.firstNext) {
            this.strategies().applyFinalOptimizers(this);
            this.firstNext = false;
        }
    }

}
