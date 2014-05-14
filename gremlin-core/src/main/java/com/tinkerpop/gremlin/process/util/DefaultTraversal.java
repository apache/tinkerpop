package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.graph.map.StartStep;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    protected final List<Step> steps = new ArrayList<>();
    protected final Optimizers optimizers = new DefaultOptimizers();
    protected final Variables variables = new DefaultVariables();
    protected boolean firstNext = true;

    public DefaultTraversal() {
        this.optimizers.register(new HolderOptimizer());
    }

    public List<Step> getSteps() {
        return this.steps;
    }

    public Variables memory() {
        return this.variables;
    }

    public Optimizers optimizers() {
        return optimizers;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
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
        return ((Holder<E>) this.steps.get(this.steps.size() - 1).next()).get();
    }

    public String toString() {
        // todo: optimizing on toString can cause weird stuff when debugging - can we have doPreflightFinalOptimization?
        //this.doFinalOptimization();
        return this.getSteps().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    public Traversal<S, E> submit(final TraversalEngine engine) {
        final Traversal<S, E> traversal = new DefaultTraversal<>();
        traversal.addStep(new StartStep<>(traversal, engine.execute(this)));
        return traversal;
    }

    private final void doFinalOptimization() {
        if (this.firstNext) {
            this.optimizers().doFinalOptimizers(this);
            this.firstNext = false;
        }
    }

}
