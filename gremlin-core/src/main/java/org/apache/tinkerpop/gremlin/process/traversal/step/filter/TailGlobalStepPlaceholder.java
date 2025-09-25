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

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * @author Matt Frantz (http://github.com/mhfrantz)
 */
public final class TailGlobalStepPlaceholder<S> extends AbstractStep<S, S> implements TailGlobalStepContract<S>, GValueHolder<S, S> {

    private GValue<Long> limit;
    private boolean bypass = false;

    public TailGlobalStepPlaceholder(final Traversal.Admin traversal, final GValue<Long> limit) {
        super(traversal);
        this.limit = limit;
        if (this.limit.isVariable()) {
            traversal.getGValueManager().register(limit);
        }
    }

    @Override
    public Traverser.Admin<S> processNextStart() {
        throw new IllegalStateException("GValueHolder is not executable");
    }

    @Override
    public void setBypass(final boolean bypass) {
        this.bypass = bypass;
    }

    @Override
    public TailGlobalStepPlaceholder<S> clone() {
        final TailGlobalStepPlaceholder<S> clone = (TailGlobalStepPlaceholder<S>) super.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.limit);
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
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), new RangeGlobalStep.RangeBiOperator<>(this.limit.get()), false, true);
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public TraverserSet<S> getEmptyBarrier() {
        return new TraverserSet<>();
    }

    @Override
    public boolean hasNextBarrier() {
        return this.starts.hasNext();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        if (!this.starts.hasNext())
            throw FastNoSuchElementException.instance();
        final TraverserSet<S> barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        while (this.starts.hasNext()) {
            barrier.add(this.starts.next());
        }
        return barrier;
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        IteratorUtils.removeOnNext(barrier.iterator()).forEachRemaining(traverser -> {
            traverser.setSideEffects(this.getTraversal().getSideEffects());
            this.addStart(traverser);
        });
    }

    @Override
    public Step<S, S> asConcreteStep() {
        TailGlobalStep<S> step = new TailGlobalStep<>(traversal, limit.get());
        TraversalHelper.copyLabels(this, step, false);
        step.setBypass(bypass);
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
