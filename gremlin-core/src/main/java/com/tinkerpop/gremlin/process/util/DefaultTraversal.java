package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;
import com.tinkerpop.gremlin.process.graph.step.map.LocalStep;

import java.util.ArrayList;
import java.util.Collections;
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
    protected TraversalStrategies strategies;
    protected TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
    protected Optional<TraversalEngine> traversalEngine = Optional.empty();

    private final StepPosition stepPosition = StepPosition.of();

    protected TraversalHolder<?, ?> traversalHolder = (TraversalHolder) EmptyStep.instance();

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
            TraversalHelper.reIdSteps(this.stepPosition, this);
            this.strategies.applyStrategies(this, engine);
            for (final Step<?, ?> step : this.getSteps()) {
                if (step instanceof TraversalHolder && !(step instanceof LocalStep)) { // TODO: why no LocalStep?
                    for (final Traversal<?, ?> nested : ((TraversalHolder<?, ?>) step).getTraversals()) {
                        nested.asAdmin().applyStrategies(engine);
                    }
                }
            }
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
        return Collections.unmodifiableList(this.steps);
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
        clone.strategies = this.strategies.clone();
        clone.lastEnd = null;
        clone.lastEndCount = 0l;
        //clone.traversalEngine = Optional.empty();
        //clone.locked = false;
        for (final Step<?, ?> step : this.steps) {
            final Step<?, ?> clonedStep = step.clone();
            clonedStep.setTraversal(clone);
            clone.steps.add(clonedStep);
        }
        TraversalHelper.reLinkSteps(clone);
        return clone;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public TraversalSideEffects getSideEffects() {
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

    @Override
    public <S2, E2> Traversal<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        if (this.getTraversalEngine().isPresent()) throw Exceptions.traversalIsLocked();
        step.setId(this.stepPosition.nextXId());

        if (this.steps.size() == 0 && index == 0)
            this.steps.add(step);
        else
            this.steps.add(index, step);

        TraversalHelper.reLinkSteps(this); // TODO: this could be faster
        return (Traversal) this;
    }

    @Override
    public <S2, E2> Traversal<S2, E2> removeStep(final int index) throws IllegalStateException {
        if (this.getTraversalEngine().isPresent()) throw Exceptions.traversalIsLocked();
        this.steps.remove(index);
        TraversalHelper.reLinkSteps(this); // TODO: this could be faster
        return (Traversal) this;
    }

    @Override
    public void setTraversalHolder(final TraversalHolder<?, ?> step) {
        this.traversalHolder = step;
    }

    @Override
    public TraversalHolder<?, ?> getTraversalHolder() {
        return this.traversalHolder;
    }

}
