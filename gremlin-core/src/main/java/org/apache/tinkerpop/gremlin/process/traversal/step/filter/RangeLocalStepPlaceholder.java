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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RangeLocalStepPlaceholder<S> extends ScalarMapStep<S,S> implements RangeLocalStepContract<S>, GValueHolder<S, S> {

    protected GValue<Long> low;
    protected GValue<Long> high;

    public RangeLocalStepPlaceholder(final Traversal.Admin traversal, final GValue<Long> low, final GValue<Long> high) {
        super(traversal);
        this.low = low;
        this.high = high;
        traversal.getGValueManager().register(low);
        traversal.getGValueManager().register(high);
    }

    @Override
    public boolean isParameterized() {
        return low.isVariable() || high.isVariable();
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (name.equals(low.getName())) {
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("The variable " + name + " must have a value of type Number");
            }
            this.low = GValue.ofLong(name, ((Number) value).longValue());
        }

        if (name.equals(high.getName())) {
            if (!(value instanceof Number)) {
                throw new IllegalArgumentException("The variable " + name + " must have a value of type Number");
            }
            this.high = GValue.ofLong(name, ((Number) value).longValue());
        }
    }

    public Long getLowRange() {
        if (low == null) {
            return null;
        }
        if (low.isVariable()) {
            this.traversal.getGValueManager().pinVariable(low.getName());
        }
        return low.get();
    }

    public Long getHighRange() {
        if (high == null) {
            return null;
        }
        if (high.isVariable()) {
            this.traversal.getGValueManager().pinVariable(high.getName());
        }
        return high.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        RangeLocalStepPlaceholder<?> that = (RangeLocalStepPlaceholder<?>) o;
        return Objects.equals(low, that.low) && Objects.equals(high, that.high);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), low, high);
    }

    /**
     * getLowRange, retaining the GValue container and without pinning the variable. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the lower bound for range().
     */
    public GValue<Long> getLowRangeAsGValue() {
        return low;
    }

    /**
     * getHighRange, retaining the GValue container and without pinning the variable. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the upper bound for range().
     */
    public GValue<Long> getHighRangeAsGValue() {
        return high;
    }

    @Override
    protected S map(Traverser.Admin<S> traverser) {
        throw new IllegalStateException("RangeLocalStepPlaceholder is not executable");
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = new HashSet<>();
        if (low.isVariable()) {
            gValues.add(low);
        }
        if (high.isVariable()) {
            gValues.add(high);
        }
        return gValues;
    }

    @Override
    public RangeLocalStep<S> asConcreteStep() {
        RangeLocalStep<S> step = new RangeLocalStep<>(traversal, low.get(), high.get());
        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public RangeLocalStepPlaceholder<S> clone() {
        return new RangeLocalStepPlaceholder<>(traversal, low, high);
    }
}
