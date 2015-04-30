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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
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
public final class StoreStep<S> extends SideEffectStep<S> implements SideEffectCapable, TraversalParent, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

    private Traversal.Admin<S, Object> storeTraversal = null;
    private String sideEffectKey;

    public StoreStep(final Traversal.Admin traversal, final String sideEffectKey) {
        super(traversal);
        this.sideEffectKey = sideEffectKey;
        this.traversal.asAdmin().getSideEffects().registerSupplierIfAbsent(this.sideEffectKey, BulkSetSupplier.instance());
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        TraversalHelper.addToCollection(
                traverser.sideEffects(this.sideEffectKey),
                TraversalUtil.applyNullable(traverser.asAdmin(), this.storeTraversal),
                traverser.bulk());
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.sideEffectKey, this.storeTraversal);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> getMapReduce() {
        return new StoreMapReduce(this);
    }

    @Override
    public List<Traversal.Admin<S, Object>> getLocalChildren() {
        return null == this.storeTraversal ? Collections.emptyList() : Collections.singletonList(this.storeTraversal);
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> storeTraversal) {
        this.storeTraversal = this.integrateChild(storeTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.SIDE_EFFECTS, TraverserRequirement.BULK);
    }

    @Override
    public StoreStep<S> clone() {
        final StoreStep<S> clone = (StoreStep<S>) super.clone();
        if (null != this.storeTraversal)
            clone.storeTraversal = clone.integrateChild(this.storeTraversal.clone());
        return clone;
    }

    ///////////////

    public static final class StoreMapReduce extends StaticMapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Collection> {

        public static final String STORE_STEP_SIDE_EFFECT_KEY = "gremlin.storeStep.sideEffectKey";

        private String sideEffectKey;
        private Supplier<Collection> collectionSupplier;

        private StoreMapReduce() {

        }

        public StoreMapReduce(final StoreStep step) {
            this.sideEffectKey = step.getSideEffectKey();
            this.collectionSupplier = step.getTraversal().asAdmin().getSideEffects().<Collection>getRegisteredSupplier(this.sideEffectKey).orElse(BulkSet::new);
        }

        @Override
        public void storeState(final Configuration configuration) {
            super.storeState(configuration);
            configuration.setProperty(STORE_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        }

        @Override
        public void loadState(final Configuration configuration) {
            this.sideEffectKey = configuration.getString(STORE_STEP_SIDE_EFFECT_KEY);
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
            keyValues.forEachRemaining(pair -> collection.add(pair.getValue()));
            return collection;
        }

        @Override
        public String getMemoryKey() {
            return this.sideEffectKey;
        }
    }

}
