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

package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.FinalGet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@Deprecated
public final class GroupStepV3d0<S, K, V, R> extends ReducingBarrierStep<S, Map<K, R>> implements ByModulating, GraphComputing, TraversalParent {

    private char state = 'k';

    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, V> valueTraversal = null;
    private Traversal.Admin<Collection<V>, R> reduceTraversal = null;

    public GroupStepV3d0(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) new GroupMapSupplierV3d0());
        this.setReducingBiOperator(new GroupBiOperatorV3d0<>());
    }

    @Override
    public Map<K, R> projectTraverser(final Traverser.Admin<S> traverser) {
        final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
        final BulkSet<V> values = new BulkSet<>();
        final V value = TraversalUtil.applyNullable(traverser, this.valueTraversal);
        TraversalHelper.addToCollectionUnrollIterator(values, value, traverser.bulk());
        return Collections.singletonMap(key, (R) values);
    }

    @Override
    public void onGraphComputer() {
        this.setSeedSupplier((Supplier) HashMapSupplier.instance());
        this.setReducingBiOperator(new GroupBiOperatorV3d0<>());
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
    public GroupStepV3d0<S, K, V, R> clone() {
        final GroupStepV3d0<S, K, V, R> clone = (GroupStepV3d0<S, K, V, R>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        if (null != this.valueTraversal)
            clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        if (null != this.reduceTraversal)
            clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final Traversal.Admin traversal : this.getLocalChildren()) {
            result ^= traversal.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.keyTraversal, this.valueTraversal, this.reduceTraversal);
    }

    public Map<K, R> getReducedMap(final Map<K, Collection<V>> valueMap) {
        final Map<K, R> reducedMap = new HashMap<>();
        for (final K key : valueMap.keySet()) {
            final R r = TraversalUtil.applyNullable(valueMap.get(key), this.reduceTraversal);
            reducedMap.put(key, r);
        }
        return reducedMap;
    }

    ////////////

    public static class GroupBiOperatorV3d0<K, V> implements BinaryOperator<Map<K, V>>, Serializable {

        private GroupBiOperatorV3d0() {

        }

        @Override
        public Map<K, V> apply(final Map<K, V> mutatingSeed, final Map<K, V> map) {
            for (final K key : map.keySet()) {
                final BulkSet<V> values = (BulkSet<V>) map.get(key);
                BulkSet<V> seedValues = (BulkSet<V>) mutatingSeed.get(key);
                if (null == seedValues) {
                    seedValues = new BulkSet<>();
                    mutatingSeed.put(key, (V) seedValues);
                }
                seedValues.addAll(values);
            }
            return mutatingSeed;
        }
    }

    ////////

    private class GroupMapV3d0 extends HashMap<K, Collection<V>> implements FinalGet<Map<K, R>> {

        @Override
        public Map<K, R> getFinal() {
            return GroupStepV3d0.this.getReducedMap(this);
        }
    }

    private class GroupMapSupplierV3d0 implements Supplier<GroupMapV3d0>, Serializable {

        private GroupMapSupplierV3d0() {
        }

        @Override
        public GroupMapV3d0 get() {
            return new GroupMapV3d0();
        }
    }
}