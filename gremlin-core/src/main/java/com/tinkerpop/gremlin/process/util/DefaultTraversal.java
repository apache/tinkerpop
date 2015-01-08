package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal<S, E>, Traversal.Admin<S, E> {

    private E lastEnd = null;
    private long lastEndCount = 0l;
    private boolean locked = false; // an optimization so getTraversalEngine().isEmpty() isn't required on each next()/hasNext()
    private Step<?, E> finalEndStep = EmptyStep.instance();

    protected List<Step> steps = new ArrayList<>();
    protected SideEffects sideEffects = new DefaultTraversalSideEffects();
    protected Optional<TraversalEngine> traversalEngine = Optional.empty();

    private TraversalStrategies strategies;

    public DefaultTraversal(final Class emanatingClass) {
        this.strategies = TraversalStrategies.GlobalCache.getStrategies(emanatingClass);
    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public void applyStrategies(final TraversalEngine engine) {
        if (!this.locked) {
            this.strategies.applyStrategies(this, engine);
            this.traversalEngine = Optional.of(engine);
            this.locked = true;
            this.finalEndStep = TraversalHelper.getEnd(this);
        }
    }

    @Override
    public Optional<TraversalEngine> getTraversalEngine() {
        return this.traversalEngine;
    }

    @Override
    public List<Step> getSteps() {
        return this.steps;
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
        if (!this.locked) this.applyStrategies(TraversalEngine.STANDARD);
        return this.lastEndCount > 0l || this.finalEndStep.hasNext();
    }

    @Override
    public E next() {
        if (!this.locked) this.applyStrategies(TraversalEngine.STANDARD);
        if (this.lastEndCount > 0l) {
            this.lastEndCount--;
            return this.lastEnd;
        } else {
            final Traverser<E> next = this.finalEndStep.next();
            final long nextBulk = next.bulk();
            if (nextBulk == 1) {
                return next.get();
            } else {
                this.lastEndCount = nextBulk - 1;
                this.lastEnd = next.get();
                return this.lastEnd;
            }
        }
    }

    @Override
    public String toString() {
        return TraversalHelper.makeTraversalString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof Iterator && TraversalHelper.areEqual(this, (Iterator) object);
    }

    @Override
    public DefaultTraversal<S, E> clone() throws CloneNotSupportedException {
        final DefaultTraversal<S, E> clone = (DefaultTraversal<S, E>) super.clone();
        clone.steps = new ArrayList<>();
        clone.sideEffects = this.sideEffects.clone();
        clone.lastEnd = null;
        clone.lastEndCount = 0l;
        //clone.traversalEngine = Optional.empty();
        //clone.locked = false;
        for (int i = this.steps.size() - 1; i >= 0; i--) {
            final Step<?, ?> clonedStep = this.steps.get(i).clone();
            clonedStep.setTraversal(clone);
            TraversalHelper.insertStep(clonedStep, 0, clone);
        }
        return clone;
    }

    @Override
    public void setSideEffects(final SideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public SideEffects getSideEffects() {
        return this.sideEffects;
    }

    @Override
    public void setStrategies(final TraversalStrategies strategies) {
        this.strategies = strategies;
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }
}
