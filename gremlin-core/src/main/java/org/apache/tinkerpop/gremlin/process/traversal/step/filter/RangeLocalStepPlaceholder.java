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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.RangeLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.RangeLocalStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Collection;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

public class RangeLocalStepPlaceholder<S> extends AbstractStep<S,S> implements GValueHolder<S, S>, RangeLocalStepInterface<S> {

    private GValue<Long> low;
    private GValue<Long> high;

    public RangeLocalStepPlaceholder(final Traversal.Admin traversal, final GValue<Long> low, final GValue<Long> high) {
        super(traversal);
        this.low = low;
        this.high = high;
        traversal.getGValueManager().track(low);
        traversal.getGValueManager().track(high);
    }

    @Override
    public Step<S, S> asConcreteStep() {
        RangeLocalStep<S> step = new RangeLocalStep<>(traversal, low.get(), high.get());
        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        return low.isVariable() || high.isVariable();
    }

    @Override
    public Long getLowRange() {
        this.traversal.getGValueManager().pinVariable(low.getName());
        return low.get();
    }

    @Override
    public Long getHighRange() {
        this.traversal.getGValueManager().pinVariable(high.getName());
        return high.get();
    }

    /**
     * getLowRange without marking the GValue as dirty and thus automatically pinning the GValue. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the lower bound for range().
     */
    public long getLowRangeGValueSafe() {
        return low.get();
    }

    /**
     * getHighRange without marking the GValue as dirty and thus automatically pinning the GValue. It is the caller's
     * responsibility to ensure that this value is not used to alter the traversal in any way which is not generalizable
     * to any parameter value.
     * @return the upper bound for range().
     */
    public long getHighRangeGValueSafe() {
        return high.get();
    }

    public String getLowName() {
        return low.getName();
    }

    public String getHighName() {
        return high.getName();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("RangeGlobalGValueContract is not executable");
    }

    @Override
    public RangeLocalStepPlaceholder<S> clone() {
        return new RangeLocalStepPlaceholder<>(traversal, low, high);
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (name.equals(low.getName())) {
            if (!(value instanceof Long)) {
                throw new IllegalArgumentException("The variable " + name + " must have a value of type Long");
            }
            this.low = GValue.ofLong(name, (Long) value);
        }

        if (name.equals(high.getName())) {
            if (!(value instanceof Long)) {
                throw new IllegalArgumentException("The variable " + name + " must have a value of type Long");
            }
            this.high = GValue.ofLong(name, (Long) value);
        }
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
}
