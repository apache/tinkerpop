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

import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.NumberHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.FinalGet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.MeanNumberSupplier;

import java.io.Serializable;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.NumberHelper.add;
import static org.apache.tinkerpop.gremlin.process.traversal.NumberHelper.div;
import static org.apache.tinkerpop.gremlin.process.traversal.NumberHelper.mul;

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
    public MapReduce<Number, Long, Number, Long, Number> getMapReduce() {
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

        public static <S extends Number> MeanGlobalBiFunction<S> instance() {
            return INSTANCE;
        }
    }

    ///////////

    private static final class MeanGlobalMapReduce extends StaticMapReduce<Number, Long, Number, Long, Number> {

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
        public Number generateFinalResult(final Iterator<KeyValue<Number, Long>> keyValues) {
            if (keyValues.hasNext()) {
                KeyValue<Number, Long> pair = keyValues.next();
                long counter = pair.getValue();
                Number result = mul(pair.getKey(), counter);
                while (keyValues.hasNext()) {
                    long incr = pair.getValue();
                    pair = keyValues.next();
                    result = add(result, mul(pair.getKey(), incr));
                    counter += incr;
                }
                return div(result, counter, true);
            }
            return Double.NaN;
        }

        public static MeanGlobalMapReduce instance() {
            return INSTANCE;
        }
    }

    ///

    public static final class MeanNumber extends Number implements Comparable<Number>, FinalGet<Number> {

        private long count;
        private Number sum;

        public MeanNumber() {
            this(0, 0);
        }

        public MeanNumber(final Number number, final long count) {
            this.count = count;
            this.sum = mul(number, count);
        }

        public MeanNumber add(final Number amount, final long count) {
            this.count += count;
            this.sum = NumberHelper.add(sum, mul(amount, count));
            return this;
        }

        public MeanNumber add(final MeanNumber other) {
            this.count += other.count;
            this.sum = NumberHelper.add(sum, other.sum);
            return this;
        }

        @Override
        public int intValue() {
            return div(this.sum, this.count).intValue();
        }

        @Override
        public long longValue() {
            return div(this.sum, this.count).longValue();
        }

        @Override
        public float floatValue() {
            return div(this.sum, this.count, true).floatValue();
        }

        @Override
        public double doubleValue() {
            return div(this.sum, this.count, true).doubleValue();
        }

        @Override
        public String toString() {
            return getFinal().toString();
        }

        @Override
        public int compareTo(final Number number) {
            // TODO: NumberHelper should provide a compareTo() implementation
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
        public Number getFinal() {
            return div(this.sum, this.count, true);
        }
    }
}