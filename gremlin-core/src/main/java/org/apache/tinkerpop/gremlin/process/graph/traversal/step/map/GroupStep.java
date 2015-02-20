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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.map;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupStep<S, K, V, R> extends ReducingBarrierStep<S, Map<K, R>> implements MapReducer, TraversalParent {

    private char state = 'k';

    private Traversal.Admin<S, K> keyTraversal = new IdentityTraversal<>();
    private Traversal.Admin<S, V> valueTraversal = new IdentityTraversal<>();
    private Traversal.Admin<Collection<V>, R> reduceTraversal = null;

    public GroupStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) new GroupMapSupplier());
        this.setBiFunction((BiFunction) new GroupReducingBiFunction());
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
    public GroupStep<S, K, V, R> clone() throws CloneNotSupportedException {
        final GroupStep<S, K, V, R> clone = (GroupStep<S, K, V, R>) super.clone();
        clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        if (null != this.reduceTraversal)
            clone.reduceTraversal = clone.integrateChild(this.reduceTraversal.clone());
        return clone;
    }

    @Override
    public MapReduce<K, Collection<V>, K, R, Map<K, R>> getMapReduce() {
        return new GroupReducingMapReduce<>(this);
    }

    @Override
    public Traverser<Map<K, R>> processNextStart() {
        if (this.byPass) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final Object[] kvPair = new Object[]{TraversalUtil.apply(traverser, (Traversal.Admin<S, Map>) this.keyTraversal), TraversalUtil.apply(traverser, (Traversal.Admin<S, Map>) this.valueTraversal)};
            return traverser.asAdmin().split(kvPair, (Step) this);
        } else {
            return super.processNextStart();
        }
    }

    ///////////

    private class GroupReducingBiFunction implements BiFunction<Map<K, Collection<V>>, Traverser.Admin<S>, Map<K, Collection<V>>>, Serializable {

        private GroupReducingBiFunction() {

        }

        @Override
        public Map<K, Collection<V>> apply(final Map<K, Collection<V>> mutatingSeed, final Traverser.Admin<S> traverser) {
            this.doGroup(traverser, mutatingSeed, keyTraversal, valueTraversal);
            return mutatingSeed;
        }

        private void doGroup(final Traverser.Admin<S> traverser, final Map<K, Collection<V>> groupMap, final Traversal.Admin<S, K> keyTraversal, final Traversal.Admin<S, V> valueTraversal) {
            final K key = TraversalUtil.apply(traverser, keyTraversal);
            final V value = TraversalUtil.apply(traverser, valueTraversal);
            Collection<V> values = groupMap.get(key);
            if (null == values) {
                values = new BulkSet<>();
                groupMap.put(key, values);
            }
            TraversalHelper.addToCollectionUnrollIterator(values, value, traverser.bulk());
        }
    }

    //////////

    private class GroupMap extends HashMap<K, Collection<V>> implements FinalGet<Map<K, R>> {

        @Override
        public Map<K, R> getFinal() {
            if (null == reduceTraversal)
                return (Map<K, R>) this;
            else {
                final Map<K, R> reduceMap = new HashMap<>();
                this.forEach((k, vv) -> reduceMap.put(k, TraversalUtil.apply(vv, reduceTraversal)));
                return reduceMap;
            }
        }
    }

    private class GroupMapSupplier implements Supplier<GroupMap>, Serializable {

        private GroupMapSupplier() {
        }

        @Override
        public GroupMap get() {
            return new GroupMap();
        }
    }

    ///////////

    public static final class GroupReducingMapReduce<K, V, R> implements MapReduce<K, Collection<V>, K, R, Map<K, R>> {

        public static final String GROUP_BY_STEP_STEP_ID = "gremlin.groupStep.stepId";

        private String groupStepId;
        private Traversal.Admin<Collection<V>, R> reduceTraversal;

        private GroupReducingMapReduce() {

        }

        public GroupReducingMapReduce(final GroupStep step) {
            this.groupStepId = step.getId();
            this.reduceTraversal = step.getReduceTraversal();
        }

        @Override
        public void storeState(final Configuration configuration) {
            MapReduce.super.storeState(configuration);
            configuration.setProperty(GROUP_BY_STEP_STEP_ID, this.groupStepId);
        }

        @Override
        public void loadState(final Configuration configuration) {
            this.groupStepId = configuration.getString(GROUP_BY_STEP_STEP_ID);
            final Traversal.Admin<?, ?> traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
            if (!traversal.isLocked())
                traversal.applyStrategies(); // TODO: this is a scary error prone requirement, but only a problem for GroupStep
            final GroupStep groupStep = new TraversalMatrix<>(traversal).getStepById(this.groupStepId);
            this.reduceTraversal = groupStep.getReduceTraversal();
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<K, Collection<V>> emitter) {
            vertex.<TraverserSet<Object[]>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> {
                final Object[] objects = traverser.get();
                emitter.emit((K) objects[0], objects[1] instanceof Collection ? (Collection<V>) objects[1] : Arrays.asList((V) objects[1]));
            }));
        }

        @Override
        public void reduce(final K key, final Iterator<Collection<V>> values, final ReduceEmitter<K, R> emitter) {
            final Set<V> set = new BulkSet<>();
            values.forEachRemaining(set::addAll);
            emitter.emit(key, null == this.reduceTraversal ? (R) set : TraversalUtil.apply(set, this.reduceTraversal));
        }

        @Override
        public Map<K, R> generateFinalResult(final Iterator<KeyValue<K, R>> keyValues) {
            final Map<K, R> map = new HashMap<>();
            keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
            return map;
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public GroupReducingMapReduce<K, V, R> clone() throws CloneNotSupportedException {
            final GroupReducingMapReduce<K, V, R> clone = (GroupReducingMapReduce<K, V, R>) super.clone();
            if (null != clone.reduceTraversal)
                clone.reduceTraversal = this.reduceTraversal.clone();
            return clone;
        }

        @Override
        public String toString() {
            return StringFactory.mapReduceString(this, this.getMemoryKey());
        }
    }

}