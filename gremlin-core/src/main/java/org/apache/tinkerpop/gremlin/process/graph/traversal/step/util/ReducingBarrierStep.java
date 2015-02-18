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
package org.apache.tinkerpop.gremlin.process.graph.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.Step;
import org.apache.tinkerpop.gremlin.process.Traversal;
import org.apache.tinkerpop.gremlin.process.Traverser;
import org.apache.tinkerpop.gremlin.process.computer.KeyValue;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.traversal.step.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ReducingBarrierStep<S, E> extends AbstractStep<S, E> implements MapReducer {

    public static final String REDUCING = Graph.Hidden.hide("reducing");

    private Supplier<E> seedSupplier;
    private BiFunction<E, Traverser<S>, E> reducingBiFunction;
    private boolean done = false;
    private boolean byPass = false;

    public ReducingBarrierStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public void setSeedSupplier(final Supplier<E> seedSupplier) {
        this.seedSupplier = seedSupplier;
    }

    public void setBiFunction(final BiFunction<E, Traverser<S>, E> reducingBiFunction) {
        this.reducingBiFunction = reducingBiFunction;
    }

    public Supplier<E> getSeedSupplier() {
        return this.seedSupplier;
    }

    public BiFunction<E, Traverser<S>, E> getBiFunction() {
        return this.reducingBiFunction;
    }

    public void byPass() {
        this.byPass = true;
    }

    @Override
    public void reset() {
        super.reset();
        this.done = false;
    }

    @Override
    public Traverser<E> processNextStart() {
        if (this.byPass) {
            return (Traverser<E>) this.starts.next();
        } else {
            if (this.done)
                throw FastNoSuchElementException.instance();
            E seed = this.seedSupplier.get();
            while (this.starts.hasNext())
                seed = this.reducingBiFunction.apply(seed, this.starts.next());
            this.done = true;
            return TraversalHelper.getRootTraversal(this.getTraversal()).getTraverserGenerator().generate(FinalGet.tryFinalGet(seed), (Step) this, 1l);
        }
    }

    @Override
    public ReducingBarrierStep<S, E> clone() throws CloneNotSupportedException {
        final ReducingBarrierStep<S, E> clone = (ReducingBarrierStep<S, E>) super.clone();
        clone.done = false;
        return clone;
    }

    @Override
    public MapReduce getMapReduce() {
        return new DefaultMapReduce();
    }

    ///////

    public class DefaultMapReduce extends StaticMapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, Object> {

        @Override
        public boolean doStage(Stage stage) {
            return !stage.equals(Stage.COMBINE);
        }

        @Override
        public String getMemoryKey() {
            return REDUCING;
        }

        @Override
        public Object generateFinalResult(final Iterator keyValues) {
            return keyValues.hasNext() ? IteratorUtils.of(((KeyValue) keyValues.next()).getValue()) : Collections.emptyIterator();

        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Object> emitter) {
            vertex.<TraverserSet<?>>property(TraversalVertexProgram.HALTED_TRAVERSERS).ifPresent(traverserSet -> traverserSet.forEach(emitter::emit));
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Object> values, final ReduceEmitter<NullObject, Object> emitter) {
            Object mutatingSeed = getSeedSupplier().get();
            final BiFunction function = getBiFunction();
            while (values.hasNext()) {
                mutatingSeed = function.apply(mutatingSeed, values.next());
            }
            emitter.emit(getTraversal().getTraverserGenerator().generate(FinalGet.tryFinalGet(mutatingSeed), (Step) getTraversal().getEndStep(), 1l));
        }

    }

    /////

    public interface FinalGet<A> {

        public A getFinal();

        public static <A> A tryFinalGet(final Object object) {
            return object instanceof FinalGet ? ((FinalGet<A>) object).getFinal() : (A) object;
        }
    }

}
