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
import org.apache.tinkerpop.gremlin.util.function.MeanNumberSupplier;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MeanGlobalStep<S extends Number, E extends Number> extends ReducingBarrierStep<S, E> implements MapReducer {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT, TraverserRequirement.BULK);

    public MeanGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) MeanNumberSupplier.instance());
        this.setBiFunction((BiFunction) MeanBiFunction.instance());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public MapReduce<MapReduce.NullObject, MeanNumber, MapReduce.NullObject, MeanNumber, Double> getMapReduce() {
        return MeanMapReduce.instance();
    }

    /////

    private static class MeanBiFunction<S extends Number> implements BiFunction<S, Traverser<S>, S>, Serializable {

        private static final MeanBiFunction INSTANCE = new MeanBiFunction();

        private MeanBiFunction() {

        }

        @Override
        public S apply(final S mutatingSeed, final Traverser<S> traverser) {
            return (S) ((MeanNumber) mutatingSeed).add(traverser.get(), traverser.bulk());
        }

        public final static <S extends Number> MeanBiFunction<S> instance() {
            return INSTANCE;
        }
    }

    ///////////

    private static class MeanMapReduce extends StaticMapReduce<MapReduce.NullObject, MeanNumber, MapReduce.NullObject, MeanNumber, Double> {

        private static final MeanMapReduce INSTANCE = new MeanMapReduce();

        private MeanMapReduce() {

        }

        @Override
        public boolean doStage(final MapReduce.Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, MeanNumber> emitter) {
            vertex.<TraverserSet<MeanNumber>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(new MeanNumber(traverser.get().doubleValue(), traverser.bulk()))));
        }

        @Override
        public void combine(final NullObject key, final Iterator<MeanNumber> values, final ReduceEmitter<NullObject, MeanNumber> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<MeanNumber> values, final ReduceEmitter<NullObject, MeanNumber> emitter) {
            MeanNumber mean = new MeanNumber();
            while (values.hasNext()) {
                mean.add(values.next());
            }
            emitter.emit(mean);
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public Double generateFinalResult(final Iterator<KeyValue<NullObject, MeanNumber>> keyValues) {
            return keyValues.hasNext() ? keyValues.next().getValue().doubleValue() : Double.NaN;
        }

        public static final MeanMapReduce instance() {
            return INSTANCE;
        }
    }

    ///

    public static final class MeanNumber extends Number implements Comparable<Number>, FinalGet<Double> {

        private long count;
        private double sum;

        public MeanNumber() {
            this(0.0d, 0l);
        }

        public MeanNumber(final double number, final long count) {
            this.count = count;
            this.sum = number * count;
        }

        public MeanNumber add(final Number amount, final long count) {
            this.count += count;
            this.sum += amount.doubleValue() * count;
            return this;
        }

        public MeanNumber add(final MeanNumber other) {
            this.count += other.count;
            this.sum += other.sum;
            return this;
        }

        @Override
        public int intValue() {
            return (int) (this.sum / this.count);
        }

        @Override
        public long longValue() {
            return (long) (this.sum / this.count);
        }

        @Override
        public float floatValue() {
            return (float) (this.sum / this.count);
        }

        @Override
        public double doubleValue() {
            return this.sum / this.count;
        }

        @Override
        public String toString() {
            return Double.toString(this.doubleValue());
        }

        @Override
        public int compareTo(final Number number) {
            return Double.valueOf(this.doubleValue()).compareTo(number.doubleValue());
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof Number && Double.valueOf(this.doubleValue()).equals(((Number) object).doubleValue());
        }

        @Override
        public int hashCode() {
            return Double.valueOf(this.doubleValue()).hashCode();
        }

        @Override
        public Double getFinal() {
            return this.doubleValue();
        }
    }
}