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

package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.util.Set;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderedTraverser<T> implements Traverser.Admin<T> {

    private Traverser.Admin<T> internal;
    private int order;

    private OrderedTraverser() {
        // for serialization
    }

    public OrderedTraverser(final Traverser.Admin<T> internal, final int order) {
        this.internal = internal instanceof OrderedTraverser ? ((OrderedTraverser) internal).internal : internal;
        this.order = order;
    }

    public int order() {
        return this.order;
    }

    @Override
    public void merge(final Admin<?> other) {
        this.internal.merge(other);
    }

    @Override
    public <R> Admin<R> split(R r, Step<T, R> step) {
        return new OrderedTraverser<>(this.internal.split(r, step), this.order);
    }

    @Override
    public Admin<T> split() {
        return new OrderedTraverser<>(this.internal.split(), this.order);
    }

    @Override
    public void addLabels(final Set<String> labels) {
        this.internal.addLabels(labels);
    }

    @Override
    public void keepLabels(final Set<String> labels) {
        this.internal.keepLabels(labels);
    }

    @Override
    public void dropLabels(final Set<String> labels) {
        this.internal.dropLabels(labels);
    }

    @Override
    public void dropPath() {
        this.internal.dropPath();
    }

    @Override
    public void set(final T t) {
        this.internal.set(t);
    }

    @Override
    public void incrLoops(final String stepLabel) {
        this.internal.incrLoops(stepLabel);
    }

    @Override
    public void resetLoops() {
        this.internal.resetLoops();
    }

    @Override
    public String getStepId() {
        return this.internal.getStepId();
    }

    @Override
    public void setStepId(final String stepId) {
        this.internal.setStepId(stepId);
    }

    @Override
    public void setBulk(final long count) {
        this.internal.setBulk(count);
    }

    @Override
    public Admin<T> detach() {
        this.internal = this.internal.detach();
        return this;
    }

    @Override
    public T attach(final Function<Attachable<T>, T> method) {
        return this.internal.attach(method);
    }

    @Override
    public Admin<T> attach(final Host host) {
        this.internal = this.internal.attach(host);
        return this;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.internal.setSideEffects(sideEffects);
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return this.internal.getSideEffects();
    }

    @Override
    public Set<String> getTags() {
        return this.internal.getTags();
    }

    @Override
    public T get() {
        return this.internal.get();
    }

    @Override
    public <S> S sack() {
        return this.internal.sack();
    }

    @Override
    public <S> void sack(final S object) {
        this.internal.sack(object);
    }

    @Override
    public Path path() {
        return this.internal.path();
    }

    @Override
    public int loops() {
        return this.internal.loops();
    }

    @Override
    public long bulk() {
        return this.internal.bulk();
    }

    @Override
    public int hashCode() {
        return this.internal.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof OrderedTraverser && ((OrderedTraverser) object).internal.equals(this.internal);
    }

    @Override
    public OrderedTraverser<T> clone() {
        try {
            final OrderedTraverser<T> clone = (OrderedTraverser<T>) super.clone();
            clone.internal = (Traverser.Admin<T>) this.internal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
