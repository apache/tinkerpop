package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Memory;
import com.tinkerpop.gremlin.process.Optimizers;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.steps.map.GraphQueryStep;
import com.tinkerpop.gremlin.process.steps.map.IdentityStep;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.util.GraphQueryBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E> {

    private final List<Step> steps = new ArrayList<>();
    private final Optimizers optimizers = new DefaultOptimizers();
    private boolean firstNext = true;

    public DefaultTraversal() {
        this.addStep(new IdentityStep(this));
    }

    public DefaultTraversal(final Graph graph) {
        this.addStep(new GraphQueryStep(this, graph, new GraphQueryBuilder(), Vertex.class));
    }

    public List<Step> getSteps() {
        return this.steps;
    }

    public Memory memory() {
        return null;
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
        if (this.firstNext) {
            this.optimizers().doFinalOptimizers(this);
            this.firstNext = false;
        }
        return this.steps.get(this.steps.size() - 1).hasNext();
    }

    public E next() {
        if (this.firstNext) {
            this.optimizers().doFinalOptimizers(this);
            this.firstNext = false;
        }
        return ((Holder<E>) this.steps.get(this.steps.size() - 1).next()).get();
    }

    public String toString() {
        return this.getSteps().toString();
    }

    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

}
