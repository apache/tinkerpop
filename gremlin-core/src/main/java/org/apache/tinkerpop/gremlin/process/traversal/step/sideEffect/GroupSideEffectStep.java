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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, ByModulating, GraphComputing {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    private Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    ///
    private String sideEffectKey;
    private boolean onGraphComputer = false;
    private GroupStep.GroupBiOperator<K, V> standardOperator = new GroupStep.GroupBiOperator<>(this.valueReduceTraversal);

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueReduceTraversal = this.integrateChild(GroupStep.convertValueTraversal(kvTraversal));
            final List<Traversal.Admin<?, ?>> splitTraversal = GroupStep.splitOnBarrierStep(this.valueReduceTraversal);
            this.valueTraversal = this.integrateChild(splitTraversal.get(0));
            this.reduceTraversal = this.integrateChild(splitTraversal.get(1));
            this.state = 'x';
            this.standardOperator = new GroupStep.GroupBiOperator<>(this.valueReduceTraversal);
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    public Optional<MemoryComputeKey> getMemoryComputeKey() {
        return Optional.of(MemoryComputeKey.of(this.getSideEffectKey(), new GroupStep.GroupBiOperator<>(), false, false));
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<K, Object> groupMap = traverser.sideEffects(this.sideEffectKey);
        final Map<K, Object> map = GroupStep.doInitialProjection(traverser.split(), this.keyTraversal, this.onGraphComputer ? this.valueTraversal : null);
        if (this.onGraphComputer)
            GroupStep.GroupBiOperator.computerInstance().apply((Map) groupMap, (Map) map);
        else
            this.standardOperator.apply((Map) groupMap, (Map) map);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal, this.valueReduceTraversal);
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> children = new ArrayList<>(4);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        children.add(this.valueReduceTraversal);
        children.add(this.valueTraversal);
        children.add(this.reduceTraversal);
        return children;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupSideEffectStep<S, K, V> clone() {
        final GroupSideEffectStep<S, K, V> clone = (GroupSideEffectStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        clone.valueReduceTraversal = clone.integrateChild(this.valueReduceTraversal.clone());
        clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueReduceTraversal.hashCode();
        return result;
    }

    @Override
    public Object generateFinalResult(final Object object) {
        return GroupStep.doFinalReduction((Map<K, Object>) object, this.onGraphComputer ? this.reduceTraversal : null);
    }
}
