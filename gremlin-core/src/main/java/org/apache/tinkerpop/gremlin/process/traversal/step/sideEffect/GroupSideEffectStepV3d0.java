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
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStepV3d0;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Deprecated
public final class GroupSideEffectStepV3d0<S, K, V, R> extends SideEffectStep<S> implements SideEffectCapable<Map<K, Collection<V>>, Map<K, R>>, TraversalParent, ByModulating {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, V> valueTraversal = null;
    private Traversal.Admin<Collection<V>, R> reduceTraversal = null;
    private String sideEffectKey;

    public GroupSideEffectStepV3d0(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey, HashMapSupplier.instance(), GroupStepV3d0.GroupBiOperatorV3d0.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final K key = TraversalUtil.applyNullable(traverser, keyTraversal);
        final V value = TraversalUtil.applyNullable(traverser, valueTraversal);
        BulkSet<V> values = new BulkSet<>();
        values.add(value, traverser.bulk());
        final Map<K, Object> map = new HashMap<>();
        map.put(key, values);
        this.getTraversal().getSideEffects().add(this.sideEffectKey, map);
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal, this.valueTraversal, this.reduceTraversal);
    }

    @Override
    public Map<K, R> generateFinalResult(final Map<K, Collection<V>> valueMap) {
        final Map<K, R> reducedMap = new HashMap<>();
        for (final K key : valueMap.keySet()) {
            final R r = TraversalUtil.applyNullable(valueMap.get(key), this.reduceTraversal);
            reducedMap.put(key, r);
        }
        return reducedMap;
    }

    @Override
    public <A, B> List<Traversal.Admin<A, B>> getLocalChildren() {
        final List<Traversal.Admin<A, B>> children = new ArrayList<>(3);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        if (null != this.valueTraversal)
            children.add((Traversal.Admin) this.valueTraversal);
        if (null != this.reduceTraversal)
            children.add((Traversal.Admin) this.reduceTraversal);
        return children;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> kvrTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvrTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(kvrTraversal);
            this.state = 'r';
        } else if ('r' == this.state) {
            this.reduceTraversal = this.integrateChild(kvrTraversal);
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
    public GroupSideEffectStepV3d0<S, K, V, R> clone() {
        final GroupSideEffectStepV3d0<S, K, V, R> clone = (GroupSideEffectStepV3d0<S, K, V, R>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = this.keyTraversal.clone();
        if (null != this.valueTraversal)
            clone.valueTraversal = this.valueTraversal.clone();
        if (null != this.reduceTraversal)
            clone.reduceTraversal = this.reduceTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.keyTraversal);
        this.integrateChild(this.valueTraversal);
        this.integrateChild(this.reduceTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        if (this.valueTraversal != null) result ^= this.valueTraversal.hashCode();
        if (this.reduceTraversal != null) result ^= this.reduceTraversal.hashCode();
        return result;
    }
}
