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
package com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.mapreduce;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraversalEngine;
import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import com.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import com.tinkerpop.gremlin.process.graph.traversal.step.sideEffect.GroupStep;
import com.tinkerpop.gremlin.process.traversal.TraversalMatrix;
import com.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import com.tinkerpop.gremlin.process.util.BulkSet;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GroupMapReduce implements MapReduce<Object, Collection, Object, Object, Map> {

    public static final String GROUP_BY_STEP_SIDE_EFFECT_KEY = "gremlin.groupStep.sideEffectKey";
    public static final String GROUP_BY_STEP_STEP_ID = "gremlin.groupStep.stepId";

    private String sideEffectKey;
    private String groupStepId;
    private Traversal.Admin reduceFunction;
    private Supplier<Map> mapSupplier;

    private GroupMapReduce() {

    }

    public GroupMapReduce(final GroupStep step) {
        this.groupStepId = step.getId();
        this.sideEffectKey = step.getSideEffectKey();
        this.reduceFunction = step.getReduceTraversal();
        this.mapSupplier = step.getTraversal().asAdmin().getSideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public void storeState(final Configuration configuration) {
        MapReduce.super.storeState(configuration);
        configuration.setProperty(GROUP_BY_STEP_SIDE_EFFECT_KEY, this.sideEffectKey);
        configuration.setProperty(GROUP_BY_STEP_STEP_ID, this.groupStepId);
    }

    @Override
    public void loadState(final Configuration configuration) {
        this.sideEffectKey = configuration.getString(GROUP_BY_STEP_SIDE_EFFECT_KEY);
        this.groupStepId = configuration.getString(GROUP_BY_STEP_STEP_ID);
        final Traversal.Admin<?, ?> traversal = TraversalVertexProgram.getTraversalSupplier(configuration).get();
        if (!traversal.getEngine().isPresent())
            traversal.applyStrategies(TraversalEngine.COMPUTER); // TODO: this is a scary error prone requirement, but only a problem for GroupStep
        final GroupStep groupStep = new TraversalMatrix<>(traversal).getStepById(this.groupStepId);
        this.reduceFunction = groupStep.getReduceTraversal();
        this.mapSupplier = traversal.getSideEffects().<Map>getRegisteredSupplier(this.sideEffectKey).orElse(HashMap::new);
    }

    @Override
    public boolean doStage(final Stage stage) {
        return !stage.equals(Stage.COMBINE);
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<Object, Collection> emitter) {
        VertexTraversalSideEffects.of(vertex).<Map<Object, Collection>>orElse(this.sideEffectKey, Collections.emptyMap()).forEach(emitter::emit);
    }

    @Override
    public void reduce(final Object key, final Iterator<Collection> values, final ReduceEmitter<Object, Object> emitter) {
        final Set set = new BulkSet<>();
        values.forEachRemaining(set::addAll);
        emitter.emit(key, (null == this.reduceFunction) ? set : TraversalUtil.apply(set, this.reduceFunction));
    }

    @Override
    public Map generateFinalResult(final Iterator<KeyValue<Object, Object>> keyValues) {
        final Map map = this.mapSupplier.get();
        keyValues.forEachRemaining(keyValue -> map.put(keyValue.getKey(), keyValue.getValue()));
        return map;
    }

    @Override
    public String getMemoryKey() {
        return this.sideEffectKey;
    }

    @Override
    public GroupMapReduce clone() throws CloneNotSupportedException {
        final GroupMapReduce clone = (GroupMapReduce) super.clone();
        if (null != clone.reduceFunction)
            clone.reduceFunction = this.reduceFunction.clone();
        return clone;
    }
}