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
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.HashMapSupplier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupCountSideEffectStep<S, E> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, MapReducer<E, Long, E, Long, Map<E, Long>> {

    private Traversal.Admin<S, E> groupTraversal = null;
    private String sideEffectKey;

    public GroupCountSideEffectStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, HashMapSupplier.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Map<Object, Long> groupCountMap = traverser.sideEffects(this.sideEffectKey);
        MapHelper.incr(groupCountMap, TraversalUtil.applyNullable(traverser.asAdmin(), this.groupTraversal), traverser.bulk());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public MapReduce<E, Long, E, Long, Map<E, Long>> getMapReduce() {
        return new GroupCountSideEffectMapReduce<>(this);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.groupTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> groupTraversal) {
        this.groupTraversal = this.integrateChild(groupTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.groupTraversal ? Collections.emptyList() : Collections.singletonList(this.groupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public GroupCountSideEffectStep<S, E> clone() {
        final GroupCountSideEffectStep<S, E> clone = (GroupCountSideEffectStep<S, E>) super.clone();
        if (null != this.groupTraversal)
            clone.groupTraversal = clone.integrateChild(this.groupTraversal.clone());
        return clone;
    }

    ///////

    public static final class GroupCountSideEffectMapReduce<E> extends StaticMapReduce<E, Long, E, Long, Map<E, Long>> {

        public static final String GROUP_COUNT_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY = "gremlin.groupCountSideEffectStep.sideEffectKey";

        private String sideEffectKey;
        private Supplier<Map<E, Long>> mapSupplier;

        private GroupCountSideEffectMapReduce() {

        }

        public GroupCountSideEffectMapReduce(final GroupCountSideEffectStep step) {
            this.sideEffectKey = step.getSideEffectKey();
            this.mapSupplier = step.getTraversal().asAdmin().getSideEffects().<Map<E, Long>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
        }

        @Override
        public void storeState(final Configuration configuration) {
            super.storeState(configuration);
            configuration.setProperty(GROUP_COUNT_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        }

        @Override
        public void loadState(final Configuration configuration) {
            this.sideEffectKey = configuration.getString(GROUP_COUNT_SIDE_EFFECT_STEP_SIDE_EFFECT_KEY);
            this.mapSupplier = TraversalVertexProgram.getTraversalSupplier(configuration).get().getSideEffects().<Map<E, Long>>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<E, Long> emitter) {
            VertexTraversalSideEffects.of(vertex).<Map<E, Number>>get(this.sideEffectKey).ifPresent(map -> map.forEach((k, v) -> emitter.emit(k, v.longValue())));
        }

        @Override
        public void reduce(final E key, final Iterator<Long> values, final ReduceEmitter<E, Long> emitter) {
            long counter = 0;
            while (values.hasNext()) {
                counter = counter + values.next();
            }
            emitter.emit(key, counter);
        }

        @Override
        public void combine(final E key, final Iterator<Long> values, final ReduceEmitter<E, Long> emitter) {
            reduce(key, values, emitter);
        }

        @Override
        public Map<E, Long> generateFinalResult(final Iterator<KeyValue<E, Long>> keyValues) {
            final Map<E, Long> map = this.mapSupplier.get();
            keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
            return map;
        }

        @Override
        public String getMemoryKey() {
            return this.sideEffectKey;
        }
    }
}
