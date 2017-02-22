/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ProjectedTraverser<T, P> implements Traverser.Admin<T> {

    private Traverser.Admin<T> baseTraverser;
    private List<P> projections;

    private ProjectedTraverser() {
        // for serialization
    }

    public ProjectedTraverser(final Traverser.Admin<T> baseTraverser, final List<P> projections) {
        this.baseTraverser = baseTraverser;
        this.projections = projections;
    }

    public List<P> getProjections() {
        return this.projections;
    }

    @Override
    public void merge(final Admin<?> other) {
        this.baseTraverser.merge(other);
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        return new ProjectedTraverser<>(this.baseTraverser.split(r, step), this.projections);
    }

    @Override
    public Admin<T> split() {
        return new ProjectedTraverser<>(this.baseTraverser.split(), this.projections);
    }

    @Override
    public void addLabels(final Set<String> labels) {
        this.baseTraverser.addLabels(labels);
    }

    @Override
    public void keepLabels(final Set<String> labels) {
        this.baseTraverser.keepLabels(labels);
    }

    @Override
    public void dropLabels(final Set<String> labels) {
        this.baseTraverser.dropLabels(labels);
    }

    @Override
    public void dropPath() {
        this.baseTraverser.dropPath();
    }

    @Override
    public void set(final T t) {
        this.baseTraverser.set(t);
    }

    @Override
    public void incrLoops(final String stepLabel) {
        this.baseTraverser.incrLoops(stepLabel);
    }

    @Override
    public void resetLoops() {
        this.baseTraverser.resetLoops();
    }

    @Override
    public String getStepId() {
        return this.baseTraverser.getStepId();
    }

    @Override
    public void setStepId(final String stepId) {
        this.baseTraverser.setStepId(stepId);
    }

    @Override
    public void setBulk(final long count) {
        this.baseTraverser.setBulk(count);
    }

    @Override
    public Admin<T> detach() {
        this.baseTraverser = this.baseTraverser.detach();
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        return this.baseTraverser.attach(method);
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.baseTraverser.setSideEffects(sideEffects);
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return this.baseTraverser.getSideEffects();
    }

    @Override
    public Set<String> getTags() {
        return this.baseTraverser.getTags();
    }

    @Override
    public T get() {
        return this.baseTraverser.get();
    }

    @Override
    public <S> S sack() {
        return this.baseTraverser.sack();
    }

    @Override
    public <S> void sack(final S object) {
        this.baseTraverser.sack(object);
    }

    @Override
    public Path path() {
        return this.baseTraverser.path();
    }

    @Override
    public int loops() {
        return this.baseTraverser.loops();
    }

    @Override
    public long bulk() {
        return this.baseTraverser.bulk();
    }

    @Override
    public int hashCode() {
        return this.baseTraverser.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof ProjectedTraverser && ((ProjectedTraverser) object).baseTraverser.equals(this.baseTraverser);
    }

    @Override
    public String toString() {
        return this.baseTraverser.toString();
    }

    @Override
    public ProjectedTraverser<T, P> clone() {
        try {
            final ProjectedTraverser<T, P> clone = (ProjectedTraverser<T, P>) super.clone();
            clone.baseTraverser = (Traverser.Admin<T>) this.baseTraverser.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public static <T> Traverser.Admin<T> tryUnwrap(final Traverser.Admin<T> traverser) {
        return traverser instanceof ProjectedTraverser ? ((ProjectedTraverser) traverser).baseTraverser : traverser;
    }
}