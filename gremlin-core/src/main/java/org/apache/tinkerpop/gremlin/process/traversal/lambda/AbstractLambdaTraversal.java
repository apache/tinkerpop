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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractLambdaTraversal<S, E> implements Traversal.Admin<S, E> {

    private static final Set<TraverserRequirement> REQUIREMENTS = Collections.singleton(TraverserRequirement.OBJECT);

    protected Traversal.Admin<S, E> bypassTraversal = null;

    public void setBypassTraversal(final Traversal.Admin<S, E> bypassTraversal) {
        this.bypassTraversal = bypassTraversal;
    }

    public Traversal.Admin<S, E> getBypassTraversal() {
        return this.bypassTraversal;
    }

    @Override
    public List<Step> getSteps() {
        return null == this.bypassTraversal ? Collections.emptyList() : this.bypassTraversal.getSteps();
    }

    @Override
    public GremlinLang getGremlinLang() {
        return null == this.bypassTraversal ? new GremlinLang() : this.bypassTraversal.getGremlinLang();
    }


    @Override
    public void reset() {
        if (null != this.bypassTraversal)
            this.bypassTraversal.reset();
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        return null == this.bypassTraversal ? (Traversal.Admin<S2, E2>) this : this.bypassTraversal.addStep(index, step);
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        return null == this.bypassTraversal ? (Traversal.Admin<S2, E2>) this : this.bypassTraversal.removeStep(index);
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        if (null != this.bypassTraversal)
            this.bypassTraversal.applyStrategies();
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return null == this.bypassTraversal ? B_O_TraverserGenerator.instance() : this.bypassTraversal.getTraverserGenerator();
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        if (null != this.bypassTraversal)
            this.bypassTraversal.setSideEffects(sideEffects);
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return null == this.bypassTraversal ? EmptyTraversalSideEffects.instance() : this.bypassTraversal.getSideEffects();
    }

    @Override
    public void setStrategies(final TraversalStrategies strategies) {
        if (null != this.bypassTraversal)
            this.bypassTraversal.setStrategies(strategies);
    }

    @Override
    public TraversalStrategies getStrategies() {
        return null == this.bypassTraversal ? EmptyTraversalStrategies.instance() : this.bypassTraversal.getStrategies();
    }

    @Override
    public void setParent(final TraversalParent step) {
        if (null != this.bypassTraversal) {
            this.bypassTraversal.setParent(step);
            step.integrateChild(this.bypassTraversal);
        }
    }

    @Override
    public TraversalParent getParent() {
        return null == this.bypassTraversal ? EmptyStep.instance() : this.bypassTraversal.getParent();
    }

    @Override
    public Traversal.Admin<S, E> clone() {
        try {
            final AbstractLambdaTraversal<S, E> clone = (AbstractLambdaTraversal<S, E>) super.clone();
            if (null != this.bypassTraversal)
                clone.bypassTraversal = this.bypassTraversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public E next() {
        if (null != this.bypassTraversal)
            return this.bypassTraversal.next();
        throw new UnsupportedOperationException("The " + this.getClass().getSimpleName() + " can only be used as a predicate traversal");
    }

    @Override
    public Traverser.Admin<E> nextTraverser() {
        if (null != this.bypassTraversal)
            return this.bypassTraversal.nextTraverser();
        throw new UnsupportedOperationException("The " + this.getClass().getSimpleName() + " can only be used as a predicate traversal");
    }

    @Override
    public boolean hasNext() {
        return null == this.bypassTraversal || this.bypassTraversal.hasNext();
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        if (null != this.bypassTraversal)
            this.bypassTraversal.addStart(start);
    }

    @Override
    public boolean isLocked() {
        return null == this.bypassTraversal || this.bypassTraversal.isLocked();
    }

    @Override
    public void lock() {
       if (this.bypassTraversal != null) bypassTraversal.lock();
    }

    /**
     * Implementations of this class can never be a root-level traversal as they are specialized implementations
     * intended to be child traversals by design.
     */
    @Override
    public boolean isRoot() {
        return false;
    }

    @Override
    public Optional<Graph> getGraph() {
        return null == this.bypassTraversal ? Optional.empty() : this.bypassTraversal.getGraph();
    }

    @Override
    public void setGraph(final Graph graph) {
        if (null != this.bypassTraversal)
            this.bypassTraversal.setGraph(graph);

    }

    @Override
    public Set<TraverserRequirement> getTraverserRequirements() {
        return null == this.bypassTraversal ? REQUIREMENTS : this.bypassTraversal.getTraverserRequirements();
    }

    @Override
    public int hashCode() {
        return null == this.bypassTraversal ? this.getClass().hashCode() : this.bypassTraversal.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return this.getClass().equals(object.getClass()) && this.hashCode() == object.hashCode();
    }

}
