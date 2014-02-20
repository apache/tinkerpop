package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Memory;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.steps.util.optimizers.DedupOptimizer;
import com.tinkerpop.gremlin.process.steps.util.optimizers.HolderOptimizer;
import com.tinkerpop.gremlin.process.steps.util.optimizers.IdentityOptimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    private final List<Step> steps = new ArrayList<>();
    private final Optimizers optimizers = new DefaultOptimizers();
    private final Memory memory = new DefaultMemory();
    private boolean firstNext = true;

    public DefaultTraversal() {
        this.optimizers.register(new HolderOptimizer());
        this.optimizers.register(new DedupOptimizer());
        this.optimizers.register(new IdentityOptimizer());
    }

    public List<Step> getSteps() {
        return this.steps;
    }

    public Memory memory() {
        return this.memory;
    }

    public Optimizers optimizers() {
        return optimizers;
    }

    public void addStarts(final Iterator<Holder<S>> starts) {
        ((Step<S, ?>) this.steps.get(0)).addStarts(starts);
    }

    public <S, E> Traversal<S, E> addStep(final Step<?, E> step) {

        if (this.steps.size() > 0) {
            step.setPreviousStep(this.steps.get(this.steps.size() - 1));
            this.steps.get(this.steps.size() - 1).setNextStep(step);
        }
        this.steps.add(step);


        return (Traversal<S, E>) this;
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
        this.doFinalOptimization();
        return this.getSteps().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    public Iterator<E> submit(final TraversalEngine engine) {
        return engine.execute(this);
    }

    private void doFinalOptimization() {
        if (this.firstNext) {
            this.optimizers().doFinalOptimizers(this);
            this.firstNext = false;
        }
    }

}
