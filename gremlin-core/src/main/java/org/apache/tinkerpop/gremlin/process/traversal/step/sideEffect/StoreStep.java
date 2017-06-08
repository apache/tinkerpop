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
 */
public final class StoreStep<S> extends SideEffectStep<S> implements SideEffectCapable<Collection, Collection>, TraversalParent, ByModulating {

    private Traversal.Admin<S, Object> storeTraversal = null;
    private String sideEffectKey;

    public StoreStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, (Supplier) BulkSetSupplier.instance(), Operator.addAll);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add(TraversalUtil.applyNullable(traverser, this.storeTraversal), traverser.bulk());
        this.getTraversal().getSideEffects().add(this.sideEffectKey, bulkSet);
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
    public StoreStep<S> clone() {
        final StoreStep<S> clone = (StoreStep<S>) super.clone();
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
