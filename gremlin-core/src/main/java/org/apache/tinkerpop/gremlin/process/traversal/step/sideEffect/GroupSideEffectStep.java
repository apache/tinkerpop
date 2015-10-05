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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ElementValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.EngineDependent;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.FinalGet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.io.Serializable;
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
public final class GroupSideEffectStep<S, K, V> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, EngineDependent, MapReducer<K, S, K, V, Map<K, V>> {

    private char state = 'k';
    private Traversal.Admin<S, K> keyTraversal = null;
    private Traversal.Admin<S, V> valueTraversal = this.integrateChild((Traversal.Admin) __.<V>fold().asAdmin());
    private String sideEffectKey;
    private boolean onGraphComputer = false;
    private FinalGroupMap<S, K, V> finalGroupMap;

    public GroupSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        if (this.onGraphComputer) {
            final Map<K, Collection<Traverser<S>>> groupMap = traverser.sideEffects(this.sideEffectKey);
            final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
            Collection<Traverser<S>> values = groupMap.get(key);
            if (null == values) {
                values = new BulkSet<>();
                groupMap.put(key, values);
            }
            values.add(traverser);
        } else {
            if (null == this.finalGroupMap) {
                final Object object = traverser.sideEffects(this.sideEffectKey);
                if (!(object instanceof FinalGroupMap))
                    traverser.sideEffects(this.sideEffectKey, this.finalGroupMap = new FinalGroupMap<>((Map) object));
            }
            final Map<K, Traversal.Admin<S, V>> groupMap = this.finalGroupMap.get();
            final K key = TraversalUtil.applyNullable(traverser, this.keyTraversal);
            Traversal.Admin<S, V> traversal = groupMap.get(key);
            if (null == traversal) {
                traversal = this.valueTraversal.clone();
                groupMap.put(key, traversal);
            }
            final Traverser.Admin<S> splitTraverser = traverser.split();
            splitTraverser.setBulk(1l);
            traversal.addStart(splitTraverser);
            TraversalHelper.getStepsOfClass(BarrierStep.class, traversal).stream().findFirst().ifPresent(BarrierStep::processAllStarts);
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
    public MapReduce<K, S, K, V, Map<K, V>> getMapReduce() {
        return new GroupSideEffectMapReduce(this);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sideEffectKey, this.keyTraversal, this.valueTraversal);
    }

    @Override
    public <A, B> List<Traversal.Admin<A, B>> getLocalChildren() {
        final List<Traversal.Admin<A, B>> children = new ArrayList<>(3);
        if (null != this.keyTraversal)
            children.add((Traversal.Admin) this.keyTraversal);
        if (null != this.valueTraversal)
            children.add((Traversal.Admin) this.valueTraversal);
        return children;
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> kvrTraversal) {
        if ('k' == this.state) {
            this.keyTraversal = this.integrateChild(kvrTraversal);
            this.state = 'v';
        } else if ('v' == this.state) {
            this.valueTraversal = this.integrateChild(kvrTraversal instanceof ElementValueTraversal ? ((Traversal.Admin) __.map(kvrTraversal).fold()) : kvrTraversal);
            this.state = 'x';
        } else {
            throw new IllegalStateException("The key and value traversals for group()-step have already been set");
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.BULK);
    }

    @Override
    public GroupSideEffectStep<S, K, V> clone() {
        final GroupSideEffectStep<S, K, V> clone = (GroupSideEffectStep<S, K, V>) super.clone();
        if (null != this.keyTraversal)
            clone.keyTraversal = clone.integrateChild(this.keyTraversal.clone());
        if (null != this.valueTraversal)
            clone.valueTraversal = clone.integrateChild(this.valueTraversal.clone());
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.sideEffectKey.hashCode();
        if (this.keyTraversal != null) result ^= this.keyTraversal.hashCode();
        if (this.valueTraversal != null) result ^= this.valueTraversal.hashCode();
        return result;
    }

    ///////////

    public static class FinalGroupMap<S, K, V> implements FinalGet<Map<K, V>>, Serializable {

        private final Map<K, Traversal.Admin<S, V>> mapTraversal;

        public FinalGroupMap(final Map<K, Traversal.Admin<S, V>> mapTraversal) {
            this.mapTraversal = mapTraversal;
        }

        public Map<K, Traversal.Admin<S, V>> get() {
            return this.mapTraversal;
        }

        @Override
        public Map<K, V> getFinal() {
            final Map<K, V> map = new HashMap<>();
            this.mapTraversal.forEach((k, v) -> {
                map.put(k, v.next());
            });
            return map;
        }
    }

    ///////////

    public static final class GroupSideEffectMapReduce<S, K, V> implements MapReduce<K, Traverser<S>, K, V, Map<K, V>> {

        public static final String GROUP_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY = "gremlin.groupSideEffectStep.sideEffectKey";
        public static final String GROUP_SIDE_EFFECT_STEP_STEP_ID = "gremlin.groupSideEffectStep.stepId";

        private String sideEffectKey;
        private String groupStepId;
        private Traversal.Admin<S, V> valueTraversal;
        private Supplier<Map<K, V>> mapSupplier;

        private GroupSideEffectMapReduce() {

        }

        public GroupSideEffectMapReduce(final GroupSideEffectStep<S, K, V> step) {
            this.groupStepId = step.getId();
            this.sideEffectKey = step.getSideEffectKey();
            this.valueTraversal = step.valueTraversal;
            this.mapSupplier = step.getTraversal().asAdmin().getSideEffects().<Map<K, V>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
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
            this.valueTraversal = groupSideEffectStep.valueTraversal.clone();
            this.mapSupplier = traversal.getSideEffects().<Map<K, V>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<K, Traverser<S>> emitter) {
            VertexTraversalSideEffects.of(vertex).<Map<K, Collection<Traverser<S>>>>get(this.sideEffectKey).ifPresent(map -> map.forEach((k, v) -> v.forEach(t -> emitter.emit(k, t))));
        }

        @Override
        public void reduce(final K key, final Iterator<Traverser<S>> values, final ReduceEmitter<K, V> emitter) {
            final Traversal.Admin<S, V> cloneValueTraversal = this.valueTraversal.clone();
            cloneValueTraversal.addStarts(values);
            emitter.emit(key, cloneValueTraversal.next());
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
                if (null != clone.valueTraversal)
                    clone.valueTraversal = this.valueTraversal.clone();
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
