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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce.GroupMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reversible;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectRegistrar;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.BulkSet;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable, SideEffectRegistrar, TraversalParent, Reversible, EngineDependent, MapReducer<Object, Collection, Object, Object, Map> {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = new IdentityTraversal<>();
    private Traversal.Admin<S, V> valueTraversal = new IdentityTraversal<>();
    private Traversal.Admin<Collection<V>, R> reduceTraversal = null;
    private String sideEffectKey;
    private boolean onGraphComputer = false;
    private Map<K, Collection<V>> tempGroupByMap;

    public GroupStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<K, Collection<V>> groupByMap = null == this.tempGroupByMap ? traverser.sideEffects(this.sideEffectKey) : this.tempGroupByMap; // for nested traversals and not !starts.hasNext()
        doGroup(traverser.asAdmin(), groupByMap, this.keyTraversal, this.valueTraversal);
        if (!this.onGraphComputer && null != this.reduceTraversal && !this.starts.hasNext()) {
            this.tempGroupByMap = groupByMap;
            final Map<K, R> reduceMap = new HashMap<>();
            doReduce(groupByMap, reduceMap, this.reduceTraversal);
            traverser.sideEffects(this.sideEffectKey, reduceMap);
        }
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void registerSideEffects() {
        if (this.sideEffectKey == null) this.sideEffectKey = this.getId();
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    private static <S, K, V> void doGroup(final Traverser.Admin<S> traverser, final Map<K, Collection<V>> groupMap, final Traversal.Admin<S, K> keyTraversal, final Traversal.Admin<S, V> valueTraversal) {
        final K key = TraversalUtil.apply(traverser, keyTraversal);
        final V value = TraversalUtil.apply(traverser, valueTraversal);
        Collection<V> values = groupMap.get(key);
        if (null == values) {
            values = new BulkSet<>();
            groupMap.put(key, values);
        }
        TraversalHelper.addToCollectionUnrollIterator(values, value, traverser.bulk());
    }

    private static <K, V, R> void doReduce(final Map<K, Collection<V>> groupMap, final Map<K, R> reduceMap, final Traversal.Admin<Collection<V>, R> reduceTraversal) {
        groupMap.forEach((k, vv) -> reduceMap.put(k, TraversalUtil.apply(vv, reduceTraversal)));
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.equals(TraversalEngine.COMPUTER);
    }

    @Override
    public MapReduce<Object, Collection, Object, Object, Map> getMapReduce() {
        return new GroupMapReduce(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.keyTraversal, this.valueTraversal, this.reduceTraversal);
    }

    @Override
    public <A, B> List<Traversal.Admin<A, B>> getLocalChildren() {
        return null == this.reduceTraversal ? (List) Arrays.asList(this.keyTraversal, this.valueTraversal) : (List) Arrays.asList(this.keyTraversal, this.valueTraversal, this.reduceTraversal);
    }

    public Traversal.Admin<Collection<V>, R> getReduceTraversal() {
        return this.reduceTraversal;
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> kvrTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvrTraversal, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(kvrTraversal, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'r';
        } else if ('r' == this.state) {
            this.reduceTraversal = this.integrateChild(kvrTraversal, TYPICAL_LOCAL_OPERATIONS);
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key, value, and reduce functions for group()-step have already been set");
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.BULK);
    }

    @Override
    public GroupStep<S, K, V, R> clone() throws CloneNotSupportedException {
        final GroupStep<S, K, V, R> clone = (GroupStep<S, K, V, R>) super.clone();
        clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        if (null != this.reduceTraversal)
            clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone(), TYPICAL_LOCAL_OPERATIONS);
        return clone;
    }
}
