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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyStep<S, E> implements Step<S, E>, TraversalParent {

    private static final EmptyStep INSTANCE = new EmptyStep<>();

    public static <S, E> Step<S, E> instance() {
        return INSTANCE;
    }

    private EmptyStep() {
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {

    }

    @Override
    public void addStart(final Traverser<S> start) {

    }

    @Override
    public void setPreviousStep(final Step<?, S> step) {

    }

    @Override
    public void reset() {

    }

    @Override
    public Step<?, S> getPreviousStep() {
        return INSTANCE;
    }

    @Override
    public void setNextStep(final Step<E, ?> step) {

    }

    @Override
    public Step<E, ?> getNextStep() {
        return INSTANCE;
    }

    @Override
    public <A, B> Traversal.Admin<A, B> getTraversal() {
        return EmptyTraversal.instance();
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> traversal) {

    }

    @Override
    public EmptyStep<S, E> clone() throws CloneNotSupportedException {
        return INSTANCE;
    }

    @Override
    public Optional<String> getLabel() {
        return Optional.empty();
    }

    @Override
    public void setLabel(final String label) {

    }

    @Override
    public void setId(final String id) {

    }

    @Override
    public String getId() {
        return Traverser.Admin.HALT;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Traverser<E> next() {
        throw FastNoSuchElementException.instance();
    }

    @Override
    public int hashCode() {
        return -1691648095;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyStep;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.emptySet();
    }
}
