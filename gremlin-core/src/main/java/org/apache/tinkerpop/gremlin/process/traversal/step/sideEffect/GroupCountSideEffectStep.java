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

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountSideEffectStep<S, E> extends SideEffectStep<S> implements SideEffectCapable<Map<E, Long>, Map<E, Long>>, TraversalParent, ByModulating, MemoryComputing<Map<E, Long>>, GraphComputing {

    private Traversal.Admin<S, E> keyTraversal = null;
    private String sideEffectKey;
    private boolean onGraphComputer = false;

    public GroupCountSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<E, Long> groupCountMap = traverser.sideEffects(this.sideEffectKey);
        final E key = TraversalUtil.applyNullable(traverser.asAdmin(), this.keyTraversal);
        MapHelper.incr(groupCountMap, this.onGraphComputer ? DetachedFactory.detach(key, true) : key, traverser.bulk());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> groupTraversal) {
        this.keyTraversal = this.integrateChild(groupTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.keyTraversal ? Collections.emptyList() : Collections.singletonList(this.keyTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupCountSideEffectStep<S, E> clone() {
        final GroupCountSideEffectStep<S, E> clone = (GroupCountSideEffectStep<S, E>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        return result;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> keyTraversal) throws UnsupportedOperationException {
        this.keyTraversal = this.integrateChild(keyTraversal);
    }

    @Override
    public MemoryComputeKey<Map<E, Long>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getSideEffectKey(), GroupCountStep.GroupCountBiOperator.instance(), false, false);
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }
}
