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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.function.ArrayListSupplier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends ReducingBarrierStep<S, E> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    public FoldStep(final Traversal.Admin traversal) {
        this(traversal, (Supplier) ArrayListSupplier.instance(), (BiFunction) ArrayListBiFunction.instance());
    }

    public FoldStep(final Traversal.Admin traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.setSeedSupplier(seed);
        this.setBiFunction(new FoldBiFunction<>(foldFunction));
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

    ///////

    public static class FoldBiFunction<S, E> implements BiFunction<E, Traverser<S>, E>, Serializable {

        private final BiFunction<E, S, E> biFunction;

        public FoldBiFunction(final BiFunction<E, S, E> biFunction) {
            this.biFunction = biFunction;
        }

        @Override
        public E apply(E seed, final Traverser<S> traverser) {
            for (int i = 0; i < traverser.bulk(); i++) {
                seed = this.biFunction.apply(seed, traverser.get());
            }
            return seed;
        }

    }
}
