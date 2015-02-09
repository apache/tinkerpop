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

import com.tinkerpop.gremlin.process.computer.KeyValue;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.computer.traversal.VertexTraversalSideEffects;
import com.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import com.tinkerpop.gremlin.process.util.metric.StandardTraversalMetrics;
import com.tinkerpop.gremlin.process.util.metric.TraversalMetrics;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public final class ProfileMapReduce extends StaticMapReduce<MapReduce.NullObject, StandardTraversalMetrics, MapReduce.NullObject, StandardTraversalMetrics, StandardTraversalMetrics> {

    public ProfileMapReduce() {

    }

    @Override
    public boolean doStage(final Stage stage) {
        return true;
    }

    @Override
    public String getMemoryKey() {
        return TraversalMetrics.METRICS_KEY;
    }

    @Override
    public void map(final Vertex vertex, final MapEmitter<NullObject, StandardTraversalMetrics> emitter) {
        VertexTraversalSideEffects.of(vertex).<StandardTraversalMetrics>ifPresent(TraversalMetrics.METRICS_KEY, emitter::emit);
    }

    @Override
    public void combine(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
        reduce(key, values, emitter);
    }

    @Override
    public void reduce(final NullObject key, final Iterator<StandardTraversalMetrics> values, final ReduceEmitter<NullObject, StandardTraversalMetrics> emitter) {
        emitter.emit(StandardTraversalMetrics.merge(values));
    }

    @Override
    public StandardTraversalMetrics generateFinalResult(final Iterator<KeyValue<NullObject, StandardTraversalMetrics>> keyValues) {
        return keyValues.next().getValue();
    }
}
