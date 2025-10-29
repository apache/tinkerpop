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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public final class TailLocalStepPlaceholder<S> extends ScalarMapStep<S, S> implements TailLocalStepContract<S>, GValueHolder<S, S> {

    private GValue<Long> limit;

    public TailLocalStepPlaceholder(final Traversal.Admin traversal, final GValue<Long> limit) {
        super(traversal);
        if (null == limit) {
            throw new IllegalArgumentException("TailLocalStepPlaceholder requires limit to be non-null");
        }
        this.limit = limit;
        if (this.limit.isVariable()) {
            traversal.getGValueManager().register(limit);
        }
    }

    @Override
    protected S map(Traverser.Admin<S> traverser) {
        throw new IllegalStateException("GValueHolder is not executable");
    }

    @Override
    public TailLocalStepPlaceholder<S> clone() {
        final TailLocalStepPlaceholder<S> clone = (TailLocalStepPlaceholder<S>) super.clone();
        clone.limit = this.limit.clone();
        return clone;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TailLocalStepPlaceholder<?> that = (TailLocalStepPlaceholder<?>) o;
        return Objects.equals(limit, that.limit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), limit);
    }

    @Override
    public Long getLimit() {
        if (limit.isVariable()) {
            traversal.getGValueManager().pinVariable(limit.getName());
        }
        return limit.get();
    }

    @Override
    public GValue<Long> getLimitAsGValue() {
        return limit;
    }

    @Override
    public TailLocalStep<S> asConcreteStep() {
        TailLocalStep<S> step = new TailLocalStep<>(traversal, limit.get());
        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        return limit.isVariable();
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (name.equals(limit.getName())) {
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("The variable " + name + " must have a value of type Number");
            }
            this.limit = GValue.ofLong(name, ((Number) value).longValue());
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return Collections.singletonList(limit);
    }
}
