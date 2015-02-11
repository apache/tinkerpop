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
package com.tinkerpop.gremlin.process.traversal.util;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.traversal.step.EmptyStep;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class EmptyTraversal<S, E> implements Traversal.Admin<S, E> {

    private static final EmptyTraversal INSTANCE = new EmptyTraversal();
    private static final TraversalSideEffects SIDE_EFFECTS = EmptyTraversalSideEffects.instance();
    private static final TraversalStrategies STRATEGIES = EmptyTraversalStrategies.instance();

    public static <A, B> EmptyTraversal<A, B> instance() {
        return INSTANCE;
    }

    protected EmptyTraversal() {

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
    public void applyStrategies(final TraversalEngine engine) {

    }

    @Override
    public Optional<TraversalEngine> getEngine() {
        return Optional.empty();
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {

    }

    @Override
    public void addStart(final Traverser<S> start) {

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
    public Traversal<S, E> submit(final GraphComputer computer) {
        return instance();
    }

    @Override
    public EmptyTraversal<S, E> clone() throws CloneNotSupportedException {
        return instance();
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
        return (TraversalParent) EmptyStep.instance();
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
}
