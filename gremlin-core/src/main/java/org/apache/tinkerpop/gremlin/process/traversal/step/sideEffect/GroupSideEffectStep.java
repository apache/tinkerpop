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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GroupStepHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, EngineDependent, MapReducer<K, Collection<?>, K, V, Map<K, V>> {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, ?> valueTraversal = this.integrateChild(__.identity().asAdmin());   // used in OLAP
    private Traversal.Admin<?, V> reduceTraversal = this.integrateChild(__.fold().asAdmin());      // used in OLAP
    private Traversal.Admin<S, V> valueReduceTraversal = this.integrateChild(__.fold().asAdmin()); // used in OLTP
    ///
    private String sideEffectKey;
    private boolean onGraphComputer = false;
    private GroupStepHelper.GroupMap<S, K, V> groupMap;
    private final Map<K, Integer> counters = new HashMap<>();

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> kvTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueReduceTraversal = this.integrateChild(GroupStepHelper.convertValueTraversal(kvTraversal));
            final List<Traversal.Admin<?, ?>> splitTraversal = GroupStepHelper.splitOnBarrierStep(this.valueReduceTraversal);
            this.valueTraversal = this.integrateChild(splitTraversal.get(0));
            this.reduceTraversal = this.integrateChild(splitTraversal.get(1));
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set: " + this);
        }
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        if (this.onGraphComputer) {      // OLAP
            final Map<K, Collection<?>> map = traverser.sideEffects(this.sideEffectKey);
            final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
            Collection<?> values = map.get(key);
            if (null == values) {
                values = new BulkSet<>();
                map.put(key, values);
            }
            this.valueTraversal.addStart(traverser.clone()); // the full traverser is provided (not a bulk 1 traverser) and clone is needed cause sideEffect steps don't split the traverser
            this.valueTraversal.fill((Collection) values);
        } else {                        // OLTP
            if (null == this.groupMap) {
                final Object object = traverser.sideEffects(this.sideEffectKey);
                if (!(object instanceof GroupStepHelper.GroupMap))
                    traverser.sideEffects(this.sideEffectKey, this.groupMap = new GroupStepHelper.GroupMap<>((Map<K, V>) object));
            }
            final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
            Traversal.Admin<S, V> traversal = this.groupMap.get(key);
            if (null == traversal) {
                traversal = this.valueReduceTraversal.clone();
                this.groupMap.put(key, traversal);
                this.counters.put(key, 0);
            }
            traversal.addStart(traverser.clone()); // this is because sideEffect steps don't split the traverser
            final int count = this.counters.compute(key, (k, i) -> ++i);
            if (count > 10000) {
                this.counters.put(key, 0);
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, traversal).ifPresent(Barrier::processAllStarts);
            }
        }
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public void onEngine(final TraversalEngine traversalEngine) {
        this.onGraphComputer = traversalEngine.isComputer();
    }

    @Override
    public MapReduce<K, Collection<?>, K, V, Map<K, V>> getMapReduce() {
        return new GroupSideEffectMapReduce<>(this);
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

    ///////////

    public static final class GroupSideEffectMapReduce<S, K, V> implements MapReduce<K, Collection<?>, K, V, Map<K, V>> {

        public static final String GROUP_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY = "gremlin.groupSideEffectStep.sideEffectKey";
        public static final String GROUP_SIDE_EFFECT_STEP_STEP_ID = "gremlin.groupSideEffectStep.stepId";

        private String sideEffectKey;
        private String groupStepId;
        private Traversal.Admin<?, V> reduceTraversal;
        private Supplier<Map<K, V>> mapSupplier;

        private GroupSideEffectMapReduce() {

        }

        public GroupSideEffectMapReduce(final GroupSideEffectStep<S, K, V> step) {
            this.groupStepId = step.getId();
            this.sideEffectKey = step.getSideEffectKey();
            this.reduceTraversal = step.reduceTraversal.clone();
            this.mapSupplier = step.getTraversal().asAdmin().getSideEffects().<Map<K, V>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMapSupplier.instance());
        }

        @Override
        public void storeState(final Configuration configuration) {
            MapReduce.super.storeState(configuration);
            configuration.setProperty(GROUP_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
            configuration.setProperty(GROUP_SIDE_EFFECT_STEP_STEP_ID, this.groupStepId);
        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.sideEffectKey = configuration.getString(GROUP_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY);
            this.groupStepId = configuration.getString(GROUP_SIDE_EFFECT_STEP_STEP_ID);
            final Traversal.Admin<?, ?> traversal = TraversalVertexProgram.getTraversal(graph, configuration);
            final GroupSideEffectStep<S, K, V> groupSideEffectStep = new TraversalMatrix<>(traversal).getStepById(this.groupStepId);
            this.reduceTraversal = groupSideEffectStep.reduceTraversal.clone();
            this.mapSupplier = traversal.getSideEffects().<Map<K, V>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMapSupplier.instance());
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<K, Collection<?>> emitter) {
            VertexTraversalSideEffects.of(vertex).<Map<K, Collection<?>>>get(this.sideEffectKey).ifPresent(map -> map.forEach(emitter::emit));
        }

        @Override
        public void reduce(final K key, final Iterator<Collection<?>> values, final ReduceEmitter<K, V> emitter) {
            Traversal.Admin<?,V> reduceTraversalClone = this.reduceTraversal.clone();
            while (values.hasNext()) {
                final BulkSet<?> value = (BulkSet<?>) values.next();
                value.forEach((v,bulk) -> reduceTraversalClone.addStart(reduceTraversalClone.getTraverserGenerator().generate(v, (Step) reduceTraversalClone.getStartStep(), bulk)));
                TraversalHelper.getFirstStepOfAssignableClass(Barrier.class, reduceTraversalClone).ifPresent(Barrier::processAllStarts);
            }
            emitter.emit(key, reduceTraversalClone.next());
        }

        @Override
        public Map<K, V> generateFinalResult(final Iterator<KeyValue<K, V>> keyValues) {
            final Map<K, V> map = this.mapSupplier.get();
            keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
            return map;
        }

        @Override
        public String getMemoryKey() {
            return this.sideEffectKey;
        }

        @Override
        public GroupSideEffectMapReduce<S, K, V> clone() {
            try {
                final GroupSideEffectMapReduce<S, K, V> clone = (GroupSideEffectMapReduce<S, K, V>) super.clone();
                clone.reduceTraversal = this.reduceTraversal.clone();
                return clone;
            } catch (final CloneNotSupportedException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public String toString() {
            return StringFactory.mapReduceString(this, this.getMemoryKey());
        }
    }
}
