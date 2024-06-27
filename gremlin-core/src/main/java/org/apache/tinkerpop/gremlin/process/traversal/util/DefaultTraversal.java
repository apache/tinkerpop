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

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.DefaultTraverserGeneratorFactory;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversal<S, E> implements Traversal.Admin<S, E> {

    private Traverser.Admin<E> lastTraverser = EmptyTraverser.instance();
    private Step<?, E> finalEndStep = EmptyStep.instance();
    private final StepPosition stepPosition = new StepPosition();
    protected transient Graph graph;
    protected transient TraversalSource g;
    protected List<Step> steps = new ArrayList<>();
    // steps will be repeatedly retrieved from this traversal so wrap them once in an immutable list that can be reused
    protected List<Step> unmodifiableSteps = Collections.unmodifiableList(steps);
    protected TraversalParent parent = EmptyStep.instance();
    protected TraversalSideEffects sideEffects = new DefaultTraversalSideEffects();
    protected TraversalStrategies strategies;
    protected transient TraverserGenerator generator;
    protected Set<TraverserRequirement> requirements;

    protected boolean locked = false;
    protected boolean closed = false;
    protected GremlinLang gremlinLang;

    private DefaultTraversal(final Graph graph, final TraversalStrategies traversalStrategies, final GremlinLang gremlinLang) {
        this.graph = graph;
        this.strategies = traversalStrategies;
        this.gremlinLang = gremlinLang;
        this.g = null;
    }

    public DefaultTraversal(final Graph graph) {
        this(graph, TraversalStrategies.GlobalCache.getStrategies(graph.getClass()), new GremlinLang());
    }

    public DefaultTraversal(final TraversalSource traversalSource) {
        this(traversalSource.getGraph(), traversalSource.getStrategies(), traversalSource.getGremlinLang());
        this.g = traversalSource;
    }

    public DefaultTraversal(final TraversalSource traversalSource, final DefaultTraversal.Admin<S,E> traversal) {
        this(traversalSource.getGraph(), traversalSource.getStrategies(), traversal.getGremlinLang());
        this.g = traversalSource;
        steps.addAll(traversal.getSteps());
    }

    public DefaultTraversal() {
        this(EmptyGraph.instance(), TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class), new GremlinLang());
    }

    public DefaultTraversal(final GremlinLang gremlinLang) {
        this(EmptyGraph.instance(), TraversalStrategies.GlobalCache.getStrategies(EmptyGraph.class), gremlinLang);
    }

    public GremlinLang getGremlinLang() {
        return this.gremlinLang;
    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        if (null == this.generator)
            this.generator = isRoot() ?
                    DefaultTraverserGeneratorFactory.instance().getTraverserGenerator(this.getTraverserRequirements()) :
                    TraversalHelper.getRootTraversal(this).getTraverserGenerator();
        return this.generator;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        if (this.locked) throw Traversal.Exceptions.traversalIsLocked();

        // apply strategies in order on a root traversal only or a traversal that is logically considered a root
        // for GraphComputer as a child of VertexProgramStep.
        if (isRoot() || this.getParent() instanceof VertexProgramStep) {
            final Iterator<TraversalStrategy<?>> strategyIterator = this.strategies.iterator();
            while (strategyIterator.hasNext()) {
                final TraversalStrategy<?> strategy = strategyIterator.next();
                TraversalHelper.applyTraversalRecursively(strategy::apply, this);
            }
        }

        // lock the traversal and its children for execution, finalizing the copy of any data from parent to child
        // as needed
        this.lock();
    }

    private void resetTraverserRequirements() {
        this.requirements = null;
        this.getTraverserRequirements();
    }

    @Override
    public Set<TraverserRequirement> getTraverserRequirements() {
        if (null == this.requirements) {
            this.requirements = EnumSet.noneOf(TraverserRequirement.class);
            for (final Step<?, ?> step : this.getSteps()) {
                this.requirements.addAll(step.getRequirements());
            }
            if (!this.requirements.contains(TraverserRequirement.LABELED_PATH) && TraversalHelper.hasLabels(this))
                this.requirements.add(TraverserRequirement.LABELED_PATH);
            if (!this.getSideEffects().keys().isEmpty())
                this.requirements.add(TraverserRequirement.SIDE_EFFECTS);
            if (null != this.getSideEffects().getSackInitialValue())
                this.requirements.add(TraverserRequirement.SACK);
            if (this.requirements.contains(TraverserRequirement.ONE_BULK))
                this.requirements.remove(TraverserRequirement.BULK);
            this.requirements = Collections.unmodifiableSet(this.requirements);
        }
        return this.requirements;
    }

    @Override
    public List<Step> getSteps() {
        return this.unmodifiableSteps;
    }

    @Override
    public Traverser.Admin<E> nextTraverser() {
        try {
            if (!this.locked) this.applyStrategies();
            if (this.lastTraverser.bulk() > 0L) {
                final Traverser.Admin<E> temp = this.lastTraverser;
                this.lastTraverser = EmptyTraverser.instance();
                return temp;
            } else {
                return this.finalEndStep.next();
            }
        } catch (final FastNoSuchElementException e) {
            throw this.isRoot() ? new NoSuchElementException() : e;
        }
    }

    @Override
    public boolean hasNext() {
        // if the traversal is closed then resources are released and there is nothing else to iterate
        if (this.isClosed()) return false;

        if (!this.locked) this.applyStrategies();
        final boolean more = this.lastTraverser.bulk() > 0L || this.finalEndStep.hasNext();
        if (!more) CloseableIterator.closeIterator(this);
        return more;
    }

    @Override
    public E next() {
        // if the traversal is closed then resources are released and there is nothing else to iterate
        if (this.isClosed())
            throw this.parent instanceof EmptyStep ? new NoSuchElementException() : FastNoSuchElementException.instance();

        try {
            if (!this.locked) this.applyStrategies();
            if (this.lastTraverser.bulk() == 0L)
                this.lastTraverser = this.finalEndStep.next();
            this.lastTraverser.setBulk(this.lastTraverser.bulk() - 1L);
            return this.lastTraverser.get();
        } catch (final FastNoSuchElementException e) {
            // No more elements will be produced by this traversal. Close this traversal
            // and release the resources.
            CloseableIterator.closeIterator(this);

            throw this.isRoot() ? new NoSuchElementException() : e;
        }
    }

    @Override
    public void reset() {
        this.steps.forEach(Step::reset);
        this.finalEndStep.reset();
        this.lastTraverser = EmptyTraverser.instance();
        this.closed = false;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        if (!this.locked) this.applyStrategies();
        if (!this.steps.isEmpty()) {
            this.steps.get(0).addStart(start);

            // match() expects that a traversal that has been iterated can continue to iterate if new starts are
            // added therefore the closed state must be reset.
            this.closed = false;
        }
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        if (!this.locked) this.applyStrategies();

        if (!this.steps.isEmpty()) {
            this.steps.get(0).addStarts(starts);

            // match() expects that a traversal that has been iterated can continue to iterate if new starts are
            // added therefore the closed state must be reset.
            this.closed = false;
        }
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
            clone.lastTraverser = EmptyTraverser.instance();
            clone.steps = new ArrayList<>();
            clone.unmodifiableSteps = Collections.unmodifiableList(clone.steps);
            clone.sideEffects = this.sideEffects.clone();
            clone.strategies = this.strategies;
            clone.gremlinLang = this.gremlinLang.clone();
            for (final Step<?, ?> step : this.steps) {
                final Step<?, ?> clonedStep = step.clone();
                clonedStep.setTraversal(clone);
                final Step previousStep = clone.steps.isEmpty() ? EmptyStep.instance() : clone.steps.get(clone.steps.size() - 1);
                clonedStep.setPreviousStep(previousStep);
                previousStep.setNextStep(clonedStep);
                clone.steps.add(clonedStep);
            }
            clone.finalEndStep = clone.getEndStep();
            clone.closed = false;
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isLocked() {
        return this.locked;
    }

    /**
     * Allow the locked property of the traversal to be directly set by those who know what they are doing. Are you
     * sure you know what you're doing?
     */
    public void setLocked(final boolean locked) {
        this.locked = locked;
    }

    @Override
    public void lock() {
        TraversalHelper.reIdSteps(stepPosition, this);

        // normalize strategies, side-effects, and Graph through children. obviously ignore the root traversal, since
        // there is no parent to draw data from and ignore stuff wrapped for GraphComputer execution and the root
        // from which to draw child data from is logically the current traversal.
        if (!isRoot() && !(parent instanceof VertexProgramStep)) {
            final TraversalParent parent = getParent();
            final Traversal.Admin parentTraversal = parent.asStep().getTraversal().asAdmin();
            this.setStrategies(parentTraversal.getStrategies());
            this.setSideEffects(parentTraversal.getSideEffects());

            // not sure why java is complaining about generics here that i have to do this??
            if (parentTraversal.getGraph().isPresent())
                this.setGraph((Graph) parentTraversal.getGraph().get());
        }

        this.finalEndStep = this.getEndStep();

        if (this.isRoot()) {
            resetTraverserRequirements();
        }

        // lock the parent before the children
        this.locked = true;

        // now lock all the children
        TraversalHelper.applyTraversalRecursively(Admin::lock, this, true);
    }

    /**
     * Determines if the traversal has been fully iterated and resources released.
     */
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void notifyClose() {
        this.closed = true;
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
        this.parent = null == step ? EmptyStep.instance() : step;
    }

    @Override
    public TraversalParent getParent() {
        return this.parent;
    }

    @Override
    public Optional<Graph> getGraph() {
        final Optional<Graph> optionalGraph =  Optional.ofNullable(this.graph);
        if (!optionalGraph.isPresent() || optionalGraph.get() == EmptyGraph.instance()) {
            final TraversalParent parent = getParent();
            if (parent != EmptyStep.instance())
                return parent.asStep().getTraversal().getGraph();
        }
        return optionalGraph;
    }

    @Override
    public Optional<TraversalSource> getTraversalSource() {
        return Optional.ofNullable(this.g);
    }

    @Override
    public void setGraph(final Graph graph) {
        this.graph = graph;
    }

    @Override
    public boolean equals(final Object other) {
        return other != null && other.getClass().equals(this.getClass()) && this.equals(((Traversal.Admin) other));
    }

    @Override
    public int hashCode() {
        int index = 0;
        int result = this.getClass().hashCode();
        for (final Step step : this.asAdmin().getSteps()) {
            result ^= Integer.rotateLeft(step.hashCode(), index++);
        }
        return result;
    }
}