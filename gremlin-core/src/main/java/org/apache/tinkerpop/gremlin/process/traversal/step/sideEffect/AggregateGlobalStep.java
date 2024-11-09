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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.BulkSetSupplier;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateGlobalStep<S> extends AbstractStep<S, S> implements SideEffectCapable<Collection, Collection>, TraversalParent, ByModulating, LocalBarrier<S> {

    private Traversal.Admin<S, Object> aggregateTraversal = null;
    private String sideEffectKey;
    private TraverserSet<S> barrier;

    public AggregateGlobalStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, (Supplier) BulkSetSupplier.instance(), Operator.addAll);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.aggregateTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> aggregateTraversal) {
        this.aggregateTraversal = this.integrateChild(aggregateTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.aggregateTraversal && this.aggregateTraversal.equals(oldTraversal))
            this.aggregateTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return null == this.aggregateTraversal ? Collections.emptyList() : Collections.singletonList(this.aggregateTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public AggregateGlobalStep<S> clone() {
        final AggregateGlobalStep<S> clone = (AggregateGlobalStep<S>) super.clone();
        clone.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        if (null != this.aggregateTraversal)
            clone.aggregateTraversal = this.aggregateTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.aggregateTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.aggregateTraversal != null)
            result ^= this.aggregateTraversal.hashCode();
        return result;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return this.barrier.remove();
    }

    @Override
    public void processAllStarts() {
        final TraversalSideEffects sideEffects = this.getTraversal().getSideEffects();

        // addAll and assign operator collect solutions as a BulkSet. And addAll merges them with an existing
        // sideEffect value which is expected to be Collection. assign replaces the existing value with the new BulkSet.
        // Therefore, they need a different treatment how they gather inputs.
        final boolean bulkReducingOperator = sideEffects.getReducer(sideEffectKey) == Operator.addAll ||
                sideEffects.getReducer(sideEffectKey) == Operator.assign;

        if (this.starts.hasNext()) {
            final BulkSet<Object> bulkSet = new BulkSet<>();

            while (this.starts.hasNext()) {
                final Traverser.Admin<S> traverser = this.starts.next();
                TraversalUtil.produce(traverser, aggregateTraversal).ifProductive(p -> bulkSet.add(p, traverser.bulk()));

                traverser.setStepId(this.getNextStep().getId());

                // when barrier is reloaded, the traversers should be at the next step
                this.barrier.add(traverser);
            }

            if (bulkReducingOperator) {
                sideEffects.add(this.sideEffectKey, bulkSet);
            } else {
                bulkSet.forEach(p -> sideEffects.add(sideEffectKey, p));
            }
        }
    }

    @Override
    public boolean hasNextBarrier() {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return !this.barrier.isEmpty();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        if (this.barrier.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<S> temp = this.barrier;
            this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
            return temp;
        }
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        this.barrier.addAll(barrier);
    }

    @Override
    public void reset() {
        super.reset();
        this.barrier.clear();
    }
}
