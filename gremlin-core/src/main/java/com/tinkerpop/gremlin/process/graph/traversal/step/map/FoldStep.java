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
package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.step.Reducing;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.util.function.ArrayListSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends ReducingBarrierStep<S, E> implements Reducing<E, S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    public FoldStep(final Traversal.Admin traversal) {
        this(traversal, (Supplier) ArrayListSupplier.instance(), (BiFunction) ArrayListBiFunction.instance());
    }

    public FoldStep(final Traversal.Admin traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.setSeedSupplier(seed);
        this.setBiFunction(new ObjectBiFunction<>(foldFunction));
    }

    @Override
    public Reducer<E, S> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), ((ObjectBiFunction<S, E>) this.getBiFunction()).getBiFunction(), false);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    /////////

    private static class ArrayListBiFunction<S> implements BiFunction<ArrayList<S>, S, ArrayList<S>>, Serializable {

        private static final ArrayListBiFunction INSTANCE = new ArrayListBiFunction();

        private ArrayListBiFunction() {

        }

        @Override
        public ArrayList<S> apply(final ArrayList<S> mutatingSeed, final S traverser) {
            mutatingSeed.add(traverser);
            return mutatingSeed;
        }

        public final static <S> ArrayListBiFunction<S> instance() {
            return INSTANCE;
        }
    }
}
