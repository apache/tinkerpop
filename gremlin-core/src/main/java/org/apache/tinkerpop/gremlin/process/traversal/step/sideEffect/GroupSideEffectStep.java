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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable<Map<K, ?>, Map<K, V>>, TraversalParent, ByModulating, GraphComputing {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    private Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    ///
    private String sideEffectKey;
    public boolean onGraphComputer = false;

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, HashMapSupplier.instance(), new GroupStep.GroupBiOperator<>(this.valueReduceTraversal));
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
            this.getTraversal().getSideEffects().register(this.sideEffectKey, null, new GroupStep.GroupBiOperator<>(this.valueReduceTraversal));
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<K, Object> map = new HashMap<>(1);
        final K key = TraversalUtil.applyNullable(traverser, keyTraversal);
        if (this.onGraphComputer) {
            final TraverserSet traverserSet = new TraverserSet();
            valueTraversal.reset();
            valueTraversal.addStart(traverser);
            valueTraversal.getEndStep().forEachRemaining(t -> traverserSet.add(t.asAdmin()));
            map.put(key, traverserSet);
        } else
            map.put(key, traverser.split());
        this.getTraversal().getSideEffects().add(this.sideEffectKey, map);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void onGraphComputer() {
        this.onGraphComputer = true;
        this.getTraversal().getSideEffects().register(this.sideEffectKey, null, GroupStep.GroupBiOperator.computerInstance());
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
            clone.keyTraversal = this.keyTraversal.clone();
        clone.valueReduceTraversal = this.valueReduceTraversal.clone();
        clone.valueTraversal = this.valueTraversal.clone();
        clone.reduceTraversal = this.reduceTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.keyTraversal);
        this.integrateChild(this.valueReduceTraversal);
        this.integrateChild(this.valueTraversal);
        this.integrateChild(this.reduceTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        result ^= this.valueReduceTraversal.hashCode();
        return result;
    }

    @Override
    public Map<K, V> generateFinalResult(final Map<K, ?> unGroupedMap) {
        return GroupStep.doFinalReduction((Map<K, Object>) unGroupedMap, this.onGraphComputer ? this.reduceTraversal : null);
    }
}
