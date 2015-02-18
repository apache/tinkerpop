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
import org.apache.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MaxGlobalStep<S extends Number> extends ReducingBarrierStep<S, S> implements MapReducer {

    public MaxGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>((S) Double.valueOf(-Double.MAX_VALUE)));
        this.setBiFunction(MaxBiFunction.<S>instance());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public MapReduce<MapReduce.NullObject, Number, MapReduce.NullObject, Number, Iterator<Traverser.Admin<Number>>> getMapReduce() {
        return new MaxMapReduce();
    }

    /////

    private static class MaxBiFunction<S extends Number> implements BiFunction<S, Traverser<S>, S>, Serializable {

        private static final MaxBiFunction INSTANCE = new MaxBiFunction();

        private MaxBiFunction() {

        }

        @Override
        public S apply(final S mutatingSeed, final Traverser<S> traverser) {
            return mutatingSeed.doubleValue() > traverser.get().doubleValue() ? mutatingSeed : traverser.get();
        }

        public final static <S extends Number> MaxBiFunction<S> instance() {
            return INSTANCE;
        }
    }

    ///////////

    private class MaxMapReduce extends StaticMapReduce<MapReduce.NullObject, Number, MapReduce.NullObject, Number, Iterator<Traverser.Admin<Number>>> {

        @Override
        public boolean doStage(final MapReduce.Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Number> emitter) {
            vertex.<TraverserSet<Number>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser.get())));
        }

        @Override
        public void combine(final NullObject key, final Iterator<Number> values, final ReduceEmitter<NullObject, Number> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Number> values, final ReduceEmitter<NullObject, Number> emitter) {
            Number max = -Double.MAX_VALUE;
            while (values.hasNext()) {
                final Number value = values.next();
                if (value.doubleValue() > max.doubleValue())
                    max = value;
            }
            emitter.emit(max);
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public Iterator<Traverser.Admin<Number>> generateFinalResult(final Iterator<KeyValue<NullObject, Number>> keyValues) {
            return IteratorUtils.of(getTraversal().getTraverserGenerator().generate(keyValues.hasNext() ? keyValues.next().getValue() : -Double.MAX_VALUE, (Step) MaxGlobalStep.this, 1L));
        }
    }
}