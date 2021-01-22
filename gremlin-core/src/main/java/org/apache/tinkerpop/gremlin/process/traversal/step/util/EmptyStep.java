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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyStep<S, E> implements Step<S, E>, TraversalParent {

    private static final EmptyStep INSTANCE = new EmptyStep<>();

    public static <S, E> EmptyStep<S, E> instance() {
        return INSTANCE;
    }

    private EmptyStep() {
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {

    }

    @Override
    public boolean hasStarts() {
        return false;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {

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
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public EmptyStep<S, E> clone() {
        return INSTANCE;
    }

    @Override
    public Set<String> getLabels() {
        return Collections.emptySet();
    }

    @Override
    public void addLabel(final String label) {

    }

    @Override
    public void removeLabel(final String label) {

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
    public Traverser.Admin<E> next() {
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
