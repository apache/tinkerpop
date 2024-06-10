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

import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraversal<S, E> implements Traversal.Admin<S, E> {

    private static final EmptyTraversal INSTANCE = new EmptyTraversal();
    private static final TraversalSideEffects SIDE_EFFECTS = EmptyTraversalSideEffects.instance();
    private static final TraversalStrategies STRATEGIES = EmptyTraversalStrategies.instance();

    public static <A, B> EmptyTraversal<A, B> instance() {
        return INSTANCE;
    }

    protected EmptyTraversal() {

    }

    public GremlinLang getGremlinLang() {
        return new GremlinLang();
    }

    @Override
    public Traversal.Admin<S, E> asAdmin() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public E next() {
        throw FastNoSuchElementException.instance();
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return SIDE_EFFECTS;
    }

    @Override
    public void applyStrategies() {

    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {

    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {

    }

    @Override
    public <E2> Traversal.Admin<S, E2> addStep(final Step<?, E2> step) {
        return instance();
    }

    @Override
    public List<Step> getSteps() {
        return Collections.emptyList();
    }

    @Override
    public EmptyTraversal<S, E> clone() {
        return instance();
    }

    @Override
    public boolean isLocked() {
        return true;
    }

    @Override
    public void lock() {
        // nothing to do here as this type of traversal is always in a locked state
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return null;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
    }

    @Override
    public TraversalStrategies getStrategies() {
        return STRATEGIES;
    }

    @Override
    public void setParent(final TraversalParent step) {

    }

    @Override
    public TraversalParent getParent() {
        return EmptyStep.instance();
    }

    @Override
    public void setStrategies(final TraversalStrategies traversalStrategies) {

    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        return (Traversal.Admin) this;
    }

    @Override
    public <S2, E2> Traversal.Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        return (Traversal.Admin) this;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyTraversal;
    }

    @Override
    public int hashCode() {
        return -343564565;
    }

    @Override
    public Set<TraverserRequirement> getTraverserRequirements() {
        return Collections.emptySet();
    }

    @Override
    public Optional<Graph> getGraph() {
        return Optional.empty();
    }

    @Override
    public void setGraph(final Graph graph) {

    }
}
