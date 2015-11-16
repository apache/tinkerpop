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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal.Admin<S, E> {

    private E lastEnd = null;
    private long lastEndCount = 0l;
    private Step<?, E> finalEndStep = EmptyStep.instance();
    private final StepPosition stepPosition = new StepPosition();
    protected transient Graph graph;
    protected List<Step> steps = new ArrayList<>();
    // steps will be repeatedly retrieved from this traversal so wrap them once in an immutable list that can be reused
    protected List<Step> unmodifiableSteps = Collections.unmodifiableList(steps);
    protected TraversalParent traversalParent = (TraversalParent) EmptyStep.instance();
    protected TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
    protected TraversalStrategies strategies;
    protected TraversalEngine traversalEngine = StandardTraversalEngine.instance(); // necessary for strategies that need the engine in OLAP message passing (not so bueno)
    protected boolean locked = false;
    protected Set<TraverserRequirement> traverserRequirements = new HashSet<>();

    public DefaultTraversal() {
        this.graph = null;
        // necessary for anonymous traversals without a graph start (rethink how this works in the future)
        this.setStrategies(TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class));
    }

    public DefaultTraversal(final Graph graph) {
        this.graph = graph;
        this.setStrategies(TraversalStrategies.GlobalCache.getStrategies(this.graph.getClass()));
    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();
        TraversalHelper.reIdSteps(this.stepPosition, this);
        this.strategies.applyStrategies(this);
        boolean hasGraph = null != this.graph;
        for (final Step<?, ?> step : this.getSteps()) {
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    globalChild.setStrategies(this.strategies);
                    globalChild.setEngine(this.traversalEngine);
                    if (hasGraph) globalChild.setGraph(this.graph);
                    globalChild.applyStrategies();
                }
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    localChild.setStrategies(this.strategies);
                    localChild.setEngine(StandardTraversalEngine.instance());
                    if (hasGraph) localChild.setGraph(this.graph);
                    localChild.applyStrategies();
                }
            }
        }
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
    public Set<TraverserRequirement> getTraverserRequirements() {
        Set<TraverserRequirement> requirements = new HashSet<>();
        for (Step step : this.getSteps()) {
            requirements.addAll(step.getRequirements());
        }

        requirements.addAll(this.traverserRequirements);
        if (this.getSideEffects().keys().size() > 0)
            requirements.add(TraverserRequirement.SIDE_EFFECTS);
        if (null != this.getSideEffects().getSackInitialValue())
            requirements.add(TraverserRequirement.SACK);
        if (this.getEngine().isComputer())
            requirements.add(TraverserRequirement.BULK);
        if (requirements.contains(TraverserRequirement.ONE_BULK))
            requirements.remove(TraverserRequirement.BULK);
        return requirements;
    }

    @Override
    public void addTraverserRequirement(final TraverserRequirement traverserRequirement) {
        this.traverserRequirements.add(traverserRequirement);
    }

    @Override
    public List<Step> getSteps() {
        return unmodifiableSteps;
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
        return StringFactory.traversalString(this);
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
    public DefaultTraversal<S, E> clone() {
        try {
            final DefaultTraversal<S, E> clone = (DefaultTraversal<S, E>) super.clone();
            clone.steps = new ArrayList<>();
            clone.unmodifiableSteps = Collections.unmodifiableList(clone.steps);
            clone.sideEffects = this.sideEffects.clone();
            clone.strategies = this.strategies.clone();
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
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
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
        this.strategies = strategies.clone();
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
        step.setTraversal(this);
        return (Traversal.Admin<S2, E2>) this;
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        if (this.locked) throw Exceptions.traversalIsLocked();
        final Step previousStep = this.steps.size() > 0 && index != 0 ? steps.get(index - 1) : null;
        final Step nextStep = this.steps.size() > index + 1 ? steps.get(index + 1) : null;
        //this.steps.get(index).setTraversal(EmptyTraversal.instance());
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

    @Override
    public Optional<Graph> getGraph() {
        return Optional.ofNullable(this.graph);
    }

    @Override
    public void setGraph(final Graph graph) {
        this.graph = graph;
    }

}
