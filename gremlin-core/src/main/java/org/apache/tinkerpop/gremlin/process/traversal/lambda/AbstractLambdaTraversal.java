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

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
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

    public List<Step> getSteps() {
        return Collections.emptyList();
    }

    public Bytecode getBytecode() {
        return new Bytecode();
    }

    @Override
    public void reset() {

    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        return (Traversal.Admin<S2, E2>) this;
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        return (Traversal.Admin<S2, E2>) this;
    }

    @Override
    public void applyStrategies() throws IllegalStateException {

    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return B_O_TraverserGenerator.instance();
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return EmptyTraversalSideEffects.instance();
    }

    @Override
    public void setStrategies(final TraversalStrategies strategies) {

    }

    @Override
    public TraversalStrategies getStrategies() {
        return EmptyTraversalStrategies.instance();
    }

    @Override
    public void setParent(final TraversalParent step) {

    }

    @Override
    public TraversalParent getParent() {
        return EmptyStep.instance();
    }

    @Override
    public Traversal.Admin<S, E> clone() {
        try {
            return (AbstractLambdaTraversal<S, E>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public E next() {
        throw new UnsupportedOperationException("The " + this.getClass().getSimpleName() + " can only be used as a predicate traversal");
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
    }

    @Override
    public boolean isLocked() {
        return true;
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.empty();
    }

    @Override
    public void setGraph(final Graph graph) {

    }

    @Override
    public Set<TraverserRequirement> getTraverserRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return this.getClass().equals(object.getClass()) && this.hashCode() == object.hashCode();
    }

}
