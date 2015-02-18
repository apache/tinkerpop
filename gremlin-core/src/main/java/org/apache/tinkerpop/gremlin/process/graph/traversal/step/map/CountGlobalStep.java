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

import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.Reducing;
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CountGlobalStep<S> extends ReducingBarrierStep<S, Long> implements Reducing<Long, Traverser<S>>, MapReducer {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.BULK);

    public CountGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>(0L));
        this.setBiFunction(CountBiFunction.<S>instance());
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public Reducer<Long, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true, true);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> getMapReduce() {
        return (MapReduce) new CountMapReduce();
    }

    ///////////

    private static class CountBiFunction<S> implements BiFunction<Long, Traverser<S>, Long>, Serializable {

        private static final CountBiFunction INSTANCE = new CountBiFunction();

        private CountBiFunction() {

        }

        @Override
        public Long apply(final Long mutatingSeed, final Traverser<S> traverser) {
            return mutatingSeed + traverser.bulk();
        }

        public final static <S> CountBiFunction<S> instance() {
            return INSTANCE;
        }
    }

    ///////////

    private class CountMapReduce extends StaticMapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Iterator<Traverser.Admin<Long>>> {

        @Override
        public boolean doStage(final MapReduce.Stage stage) {
            return true;
        }

        @Override
        public String getMemoryKey() {
            return Graph.Hidden.hide("reducingBarrier");
        }

        @Override
        public Iterator<Traverser.Admin<Long>> generateFinalResult(final Iterator<KeyValue<NullObject, Long>> keyValues) {
            return IteratorUtils.of(getTraversal().getTraverserGenerator().generate(keyValues.hasNext() ? keyValues.next().getValue() : 0L, (Step) CountGlobalStep.this, 1L));
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Long> emitter) {
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser.bulk())));
        }

        @Override
        public void combine(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Long> values, final ReduceEmitter<NullObject, Long> emitter) {
            long count = 0l;
            while (values.hasNext()) {
                count = count + values.next();
            }
            emitter.emit(count);
        }
    }

}
