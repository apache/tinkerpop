/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal.Admin<S, E> {

    private E lastEnd = null;
    private long lastEndCount = 0l;
    private Step<?, E> finalEndStep = EmptyStep.instance();
    private final StepPosition stepPosition = new StepPosition();

    protected List<Step> steps = new ArrayList<>();
    protected TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
    protected TraversalStrategies strategies;
    protected TraversalEngine traversalEngine;

    protected boolean locked = false;

    protected TraversalParent traversalParent = (TraversalParent) EmptyStep.instance();

    public DefaultTraversal(final Object emanatingObject) {
        this.setStrategies(TraversalStrategies.GlobalCache.getStrategies(emanatingObject.getClass()));
        if (emanatingObject instanceof Graph)
            this.traversalEngine = ((Graph) emanatingObject).engine();
        else if (emanatingObject instanceof Element)
            this.traversalEngine = ((Element) emanatingObject).graph().engine();
        else
            this.traversalEngine = StandardTraversalEngine.instance();
    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        TraversalHelper.reIdSteps(this.stepPosition, this);
        this.strategies.applyStrategies(this, this.traversalEngine);
        for (final Step<?, ?> step : this.getSteps()) {
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    globalChild.setStrategies(this.strategies);
                    globalChild.setEngine(this.traversalEngine);
                    globalChild.applyStrategies();
                }
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    localChild.setStrategies(this.strategies);
                    localChild.setEngine(StandardTraversalEngine.instance());
                    localChild.applyStrategies();
                }
            }
        }
        this.traversalEngine.processTraversal(this);
        this.finalEndStep = this.getEndStep();
        this.locked = true;
    }

    @Override
    public TraversalEngine getEngine() {
        return this.traversalEngine;
    }

    @Override
    public void setEngine(final TraversalEngine engine) {
        this.traversalEngine = engine;
    }

    @Override
    public List<Step> getSteps() {
        return Collections.unmodifiableList(this.steps);
    }

    @Override
    public boolean hasNext() {
        if (!this.locked) this.applyStrategies();
        return this.lastEndCount > 0l || this.finalEndStep.hasNext();
    }

    @Override
    public E next() {
        if (!this.locked) this.applyStrategies();
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
    public void reset() {
        this.steps.forEach(Step::reset);
        this.lastEndCount = 0l;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        if (!this.locked) this.applyStrategies();
        if (!this.steps.isEmpty()) this.steps.get(0).addStart(start);
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {
        if (!this.locked) this.applyStrategies();
        if (!this.steps.isEmpty()) this.steps.get(0).addStarts(starts);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeTraversalString(this);
    }

    @Override
    public Step<S, ?> getStartStep() {
        return this.steps.isEmpty() ? EmptyStep.instance() : this.steps.get(0);
    }

    @Override
    public Step<?, E> getEndStep() {
        return this.steps.isEmpty() ? EmptyStep.instance() : this.steps.get(this.steps.size() - 1);
    }

    @Override
    public DefaultTraversal<S, E> clone() throws CloneNotSupportedException {
        final DefaultTraversal<S, E> clone = (DefaultTraversal<S, E>) super.clone();
        clone.steps = new ArrayList<>();
        clone.sideEffects = this.sideEffects.clone();
        clone.strategies = this.strategies.clone(); // TODO: does this need to be cloned?
        clone.lastEnd = null;
        clone.lastEndCount = 0l;
        for (final Step<?, ?> step : this.steps) {
            final Step<?, ?> clonedStep = step.clone();
            clonedStep.setTraversal(clone);
            final Step previousStep = clone.steps.isEmpty() ? EmptyStep.instance() : clone.steps.get(clone.steps.size() - 1);
            clonedStep.setPreviousStep(previousStep);
            previousStep.setNextStep(clonedStep);
            clone.steps.add(clonedStep);
        }
        clone.finalEndStep = clone.getEndStep();
        return clone;
    }

    @Override
    public boolean isLocked() {
        return this.locked;
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
        try {
            this.strategies = strategies.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public TraversalStrategies getStrategies() {
        return this.strategies;
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        if (this.locked) throw Exceptions.traversalIsLocked();
        step.setId(this.stepPosition.nextXId());
        this.steps.add(index, step);
        final Step previousStep = this.steps.size() > 0 && index != 0 ? steps.get(index - 1) : null;
        final Step nextStep = this.steps.size() > index + 1 ? steps.get(index + 1) : null;
        step.setPreviousStep(null != previousStep ? previousStep : EmptyStep.instance());
        step.setNextStep(null != nextStep ? nextStep : EmptyStep.instance());
        if (null != previousStep) previousStep.setNextStep(step);
        if (null != nextStep) nextStep.setPreviousStep(step);
        return (Traversal.Admin<S2, E2>) this;
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        if (this.locked) throw Exceptions.traversalIsLocked();
        final Step previousStep = this.steps.size() > 0 && index != 0 ? steps.get(index - 1) : null;
        final Step nextStep = this.steps.size() > index + 1 ? steps.get(index + 1) : null;
        this.steps.remove(index);
        if (null != previousStep) previousStep.setNextStep(null == nextStep ? EmptyStep.instance() : nextStep);
        if (null != nextStep) nextStep.setPreviousStep(null == previousStep ? EmptyStep.instance() : previousStep);
        return (Traversal.Admin<S2, E2>) this;
    }

    @Override
    public void setParent(final TraversalParent step) {
        this.traversalParent = step;
    }

    @Override
    public TraversalParent getParent() {
        return this.traversalParent;
    }

}
