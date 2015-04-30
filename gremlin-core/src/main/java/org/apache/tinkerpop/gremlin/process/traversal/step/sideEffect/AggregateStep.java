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
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.BulkSetSupplier;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AggregateStep<S> extends CollectingBarrierStep<S> implements SideEffectCapable, TraversalParent, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private Traversal.Admin<S, Object> aggregateTraversal = null;
    private String sideEffectKey;

    public AggregateStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.getTraversal().asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSetSupplier.instance());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.aggregateTraversal);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new AggregateMapReduce(this);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> aggregateTraversal) {
        this.aggregateTraversal = this.integrateChild(aggregateTraversal);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return null == this.aggregateTraversal ? Collections.emptyList() : Collections.singletonList(this.aggregateTraversal);
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        traverserSet.forEach(traverser ->
                TraversalHelper.addToCollection(
                        traverser.getSideEffects().<Collection<Object>>get(this.sideEffectKey).get(),
                        TraversalUtil.applyNullable(traverser, this.aggregateTraversal),
                        traverser.bulk()));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public AggregateStep<S> clone() {
        final AggregateStep<S> clone = (AggregateStep<S>) super.clone();
        if (null != this.aggregateTraversal)
            clone.aggregateTraversal = clone.integrateChild(this.aggregateTraversal.clone());
        return clone;
    }

    ////////

    public static final class AggregateMapReduce extends StaticMapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

        public static final String AGGREGATE_STEP_SIDE_EFFECT_KEY = "gremlin.aggregateStep.sideEffectKey";

        private String sideEffectKey;
        private Supplier<Collection> collectionSupplier;

        private AggregateMapReduce() {

        }

        public AggregateMapReduce(final AggregateStep step) {
            this.sideEffectKey = step.getSideEffectKey();
            this.collectionSupplier = step.getTraversal().asAdmin().getSideEffects().<Collection>getRegisteredSupplier(this.sideEffectKey).orElse(BulkSet::new);
        }

        @Override
        public void storeState(final Configuration configuration) {
            super.storeState(configuration);
            configuration.setProperty(AGGREGATE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        }

        @Override
        public void loadState(final Configuration configuration) {
            this.sideEffectKey = configuration.getString(AGGREGATE_STEP_SIDE_EFFECT_KEY);
            this.collectionSupplier = TraversalVertexProgram.getTraversalSupplier(configuration).get().getSideEffects().<Collection>getRegisteredSupplier(this.sideEffectKey).orElse(BulkSet::new);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
            VertexTraversalSideEffects.of(vertex).<Collection<?>>get(this.sideEffectKey).ifPresent(list -> list.forEach(emitter::emit));
        }

        @Override
        public Collection generateFinalResult(final Iterator<KeyValue<NullObject, Object>> keyValues) {
            final Collection collection = this.collectionSupplier.get();
            keyValues.forEachRemaining(keyValue -> collection.add(keyValue.getValue()));
            return collection;
        }

        @Override
        public String getMemoryKey() {
            return this.sideEffectKey;
        }
    }
}
