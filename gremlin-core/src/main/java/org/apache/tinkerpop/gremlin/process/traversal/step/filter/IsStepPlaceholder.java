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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

public final class IsStepPlaceholder<S> extends FilterStep<S> implements GValueHolder<S, S>, IsStepContract<S> {

    private P<S> predicate;

    public IsStepPlaceholder(final Traversal.Admin traversal, final P<S> predicate) {
        super(traversal);
        this.predicate = predicate;
        traversal.getGValueManager().register(predicate.getGValues());
    }

    @Override
    protected boolean filter(Traverser.Admin<S> traverser) {
        throw new IllegalStateException("IsStepPlaceholder is not executable");
    }

    @Override
    public P<S> getPredicate() {
        if (predicate.isParameterized()) {
            traversal.getGValueManager().pinGValues(predicate.getGValues());
        }
        return this.predicate;
    }

    @Override
    public P<S> getPredicateGValueSafe() {
        return this.predicate;
    }

    @Override
    public IsStepPlaceholder<S> clone() {
        final IsStepPlaceholder<S> clone = (IsStepPlaceholder<S>) super.clone();
        clone.predicate = this.predicate.clone();
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IsStepPlaceholder<?> that = (IsStepPlaceholder<?>) o;
        return Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), predicate);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return EnumSet.of(TraverserRequirement.OBJECT);
    }

    @Override
    public IsStep<S> asConcreteStep() {
        IsStep<S> step = new IsStep<>(traversal, predicate);
        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        return predicate.isParameterized();
    }

    @Override
    public void updateVariable(String name, Object value) {
        predicate.updateVariable(name, (S) value);
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return predicate.getGValues();
    }
}
