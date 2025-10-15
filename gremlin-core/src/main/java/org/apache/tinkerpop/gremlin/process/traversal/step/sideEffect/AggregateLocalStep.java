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
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.BulkSetSupplier;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.7.5, use local() with aggregate() instead
 */
@Deprecated
public final class AggregateLocalStep<S> extends SideEffectStep<S> implements SideEffectCapable<Collection, Collection>, TraversalParent, ByModulating {

    private Traversal.Admin<S, Object> storeTraversal = null;
    private String sideEffectKey;

    public AggregateLocalStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, (Supplier) BulkSetSupplier.instance(), Operator.addAll);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final TraversalSideEffects sideEffects = this.getTraversal().getSideEffects();
        // addAll and assign operator collect solutions as a BulkSet. And addAll merges them with an existing
        // sideEffect value which is expected to be Collection. assign replaces the existing value with the new BulkSet.
        // Therefore, they need a different treatment how they gather inputs.
        final boolean isOperatorForBulkSet = sideEffects.getReducer(sideEffectKey) == Operator.addAll ||
                sideEffects.getReducer(sideEffectKey) == Operator.assign;

        final BulkSet<Object> bulkSet = new BulkSet<>();
        TraversalUtil.produce(traverser, this.storeTraversal).ifProductive(p -> bulkSet.add(p, traverser.bulk()));

        if (isOperatorForBulkSet) {
            sideEffects.add(this.sideEffectKey, bulkSet);
        } else {
            bulkSet.forEach(p -> sideEffects.add(sideEffectKey, p));
        }
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.storeTraversal);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return null == this.storeTraversal ? Collections.emptyList() : Collections.singletonList(this.storeTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> storeTraversal) {
        this.storeTraversal = this.integrateChild(storeTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.storeTraversal && this.storeTraversal.equals(oldTraversal))
            this.storeTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.BULK);
    }

    @Override
    public AggregateLocalStep<S> clone() {
        final AggregateLocalStep<S> clone = (AggregateLocalStep<S>) super.clone();
        if (null != this.storeTraversal)
            clone.storeTraversal = this.storeTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.storeTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.storeTraversal != null)
            result ^= this.storeTraversal.hashCode();
        return result;
    }
}
