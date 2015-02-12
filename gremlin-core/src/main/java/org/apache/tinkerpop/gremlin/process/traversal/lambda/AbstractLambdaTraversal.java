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
package com.tinkerpop.gremlin.process.traversal.lambda;

import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import com.tinkerpop.gremlin.process.traverser.O_TraverserGenerator;
import com.tinkerpop.gremlin.process.traversal.step.EmptyStep;
import com.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import com.tinkerpop.gremlin.process.traversal.util.EmptyTraversalStrategies;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractLambdaTraversal<S, E> implements Traversal.Admin<S, E> {

    @Override
    public List<Step> getSteps() {
        return Collections.emptyList();
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
    public void applyStrategies(final TraversalEngine engine) throws IllegalStateException {

    }

    @Override
    public Optional<TraversalEngine> getEngine() {
        return Optional.of(TraversalEngine.STANDARD);
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        return O_TraverserGenerator.instance();
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
        return (TraversalParent) EmptyStep.instance();
    }

    @Override
    public Traversal.Admin<S, E> clone() throws CloneNotSupportedException {
        return (AbstractLambdaTraversal<S, E>) super.clone();
    }

    @Override
    public E next() {
        throw new UnsupportedOperationException("The " + this.getClass().getSimpleName() + " can only be used as a predicate traversal");
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    public void addStart(final Traverser<S> start) {
    }

}
