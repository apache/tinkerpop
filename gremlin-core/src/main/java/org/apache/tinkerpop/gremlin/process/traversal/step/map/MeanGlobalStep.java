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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.FinalGet;
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
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class MeanGlobalStep<S extends Number, E extends Number> extends ReducingBarrierStep<S, E> implements MapReducer {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT, TraverserRequirement.BULK);

    public MeanGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier((Supplier) MeanNumberSupplier.instance());
        this.setBiFunction((BiFunction) MeanGlobalBiFunction.instance());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public MapReduce<Number, Long, Number, Long, Double> getMapReduce() {
        return MeanGlobalMapReduce.instance();
    }

    /////

    private static class MeanGlobalBiFunction<S extends Number> implements BiFunction<S, Traverser<S>, S>, Serializable {

        private static final MeanGlobalBiFunction INSTANCE = new MeanGlobalBiFunction();

        private MeanGlobalBiFunction() {

        }

        @Override
        public S apply(final S mutatingSeed, final Traverser<S> traverser) {
            return (S) ((MeanNumber) mutatingSeed).add(traverser.get(), traverser.bulk());
        }

        public final static <S extends Number> MeanGlobalBiFunction<S> instance() {
            return INSTANCE;
        }
    }

    ///////////

    private static final class MeanGlobalMapReduce extends StaticMapReduce<Number, Long, Number, Long, Double> {

        private static final MeanGlobalMapReduce INSTANCE = new MeanGlobalMapReduce();

        private MeanGlobalMapReduce() {

        }

        @Override
        public boolean doStage(final MapReduce.Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Number, Long> emitter) {
            vertex.<TraverserSet<Number>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(traverser -> emitter.emit(traverser.get(), traverser.bulk())));
        }

        @Override
        public void combine(final Number key, final Iterator<Long> values, final ReduceEmitter<Number, Long> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final Number key, final Iterator<Long> values, final ReduceEmitter<Number, Long> emitter) {
            long counter = 0;
            while (values.hasNext()) {
                counter = counter + values.next();
            }
            emitter.emit(key, counter);
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public Double generateFinalResult(final Iterator<KeyValue<Number, Long>> keyValues) {
            if (keyValues.hasNext()) {
                KeyValue<Number, Long> pair = keyValues.next();
                double result = pair.getKey().doubleValue() * pair.getValue();
                long counter = pair.getValue();
                while (keyValues.hasNext()) {
                    pair = keyValues.next();
                    result += pair.getKey().doubleValue() * pair.getValue();
                    counter += pair.getValue();
                }
                return result / counter;
            }
            return Double.NaN;
        }

        public static final MeanGlobalMapReduce instance() {
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