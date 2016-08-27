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
package org.apache.tinkerpop.gremlin.process.remote.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractRemoteTraversal<S,E> implements RemoteTraversal<S,E> {
    @Override
    public Bytecode getBytecode() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public List<Step> getSteps() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S2, E2> Admin<S2, E2> addStep(final int index, final Step<?, ?> step) throws IllegalStateException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S2, E2> Admin<S2, E2> removeStep(final int index) throws IllegalStateException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void applyStrategies() throws IllegalStateException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public TraverserGenerator getTraverserGenerator() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public Set<TraverserRequirement> getTraverserRequirements() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void setStrategies(final TraversalStrategies strategies) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public TraversalStrategies getStrategies() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void setParent(final TraversalParent step) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public TraversalParent getParent() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public Admin<S, E> clone() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public boolean isLocked() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public Optional<Graph> getGraph() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void setGraph(final Graph graph) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }
}
