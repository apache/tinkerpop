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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ReducingBarrierStep<S, E> extends AbstractStep<S, E> implements MapReducer, Barrier {

    public static final String REDUCING = Graph.Hidden.hide("reducing");

    protected Supplier<E> seedSupplier;
    protected BiFunction<E, Traverser<S>, E> reducingBiFunction;
    private boolean done = false;

    private E seed = null;

    public ReducingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    public void setBiFunction(final BiFunction<E, Traverser<S>, E> reducingBiFunction) {
        this.reducingBiFunction = reducingBiFunction;
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
        this.seed = null;
    }

    @Override
    public void addStarts(final Iterator<Traverser<S>> starts) {
        if (starts.hasNext()) {
            this.done = false;
            super.addStarts(starts);
        }
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.done = false;
        super.addStart(start);
    }

    @Override
    public void processAllStarts() {
        if (this.seed == null) this.seed = this.seedSupplier.get();
        while (this.starts.hasNext())
            this.seed = this.reducingBiFunction.apply(this.seed, this.starts.next());
    }

    @Override
    public Traverser<E> processNextStart() {
        if (this.done)
            throw FastNoSuchElementException.instance();
        this.processAllStarts();
        this.done = true;
        final Traverser<E> traverser = TraversalHelper.getRootTraversal(this.getTraversal()).getTraverserGenerator().generate(FinalGet.tryFinalGet(this.seed), (Step) this, 1l);
        this.seed = null;
        return traverser;
    }

    @Override
    public ReducingBarrierStep<S, E> clone() {
        final ReducingBarrierStep<S, E> clone = (ReducingBarrierStep<S, E>) super.clone();
        clone.done = false;
        clone.seed = null;
        return clone;
    }

    @Override
    public MapReduce getMapReduce() {
        return new DefaultMapReduce(this.seedSupplier, this.reducingBiFunction);
    }

    ///////

    public static class DefaultMapReduce extends StaticMapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Object> {

        private static final String REDUCING_BARRIER_STEP_SEED_SUPPLIER = "gremlin.reducingBarrierStep.seedSupplier";
        private static final String REDUCING_BARRIER_STEP_BI_FUNCTION = "gremlin.reducingBarrierStep.biFunction";

        private BiFunction biFunction;
        private Supplier seedSupplier;

        private DefaultMapReduce() {
        }

        public DefaultMapReduce(final Supplier seedSupplier, final BiFunction biFunction) {
            this.seedSupplier = seedSupplier;
            this.biFunction = biFunction;

        }

        @Override
        public void storeState(final Configuration configuration) {
            super.storeState(configuration);
            VertexProgramHelper.serialize(this.seedSupplier, configuration, REDUCING_BARRIER_STEP_SEED_SUPPLIER);
            VertexProgramHelper.serialize(this.biFunction, configuration, REDUCING_BARRIER_STEP_BI_FUNCTION);

        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.seedSupplier = VertexProgramHelper.deserialize(configuration, REDUCING_BARRIER_STEP_SEED_SUPPLIER);
            this.biFunction = VertexProgramHelper.deserialize(configuration, REDUCING_BARRIER_STEP_BI_FUNCTION);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public Object generateFinalResult(final Iterator keyValues) {
            return ((KeyValue) keyValues.next()).getValue();

        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(emitter::emit));
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Object> values, final ReduceEmitter<NullObject, Object> emitter) {
            Object mutatingSeed = this.seedSupplier.get();
            while (values.hasNext()) {
                mutatingSeed = this.biFunction.apply(mutatingSeed, values.next());
            }
            emitter.emit(FinalGet.tryFinalGet(mutatingSeed));
        }
    }
}
