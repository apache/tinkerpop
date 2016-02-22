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
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends ReducingBarrierStep<S, E> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    public FoldStep(final Traversal.Admin traversal) {
        this(traversal, (Supplier) ArrayListSupplier.instance(), (BiFunction) new ListBiOperator<>());
    }

    @Override
    public E projectTraverser(final Traverser.Admin<S> traverser) {
        return (E) Collections.singletonList(traverser.get());
    }

    public FoldStep(final Traversal.Admin traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.setSeedSupplier(seed);
        this.setReducingBiOperator(new FoldBiOperator<>(foldFunction));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    /////////

    private static class ListBiOperator<S> implements BinaryOperator<List<S>>, Serializable {

        @Override
        public List<S> apply(final List<S> mutatingSeed, final List<S> list) {
            mutatingSeed.addAll(list);
            return mutatingSeed;
        }

    }

    ///////

    public static class FoldBiOperator<E> implements BinaryOperator<E>, Serializable {

        private final BiFunction biFunction;

        public FoldBiOperator(final BiFunction biFunction) {
            this.biFunction = biFunction;
        }

        @Override
        public E apply(E seed, E other) {
            return (E) this.biFunction.apply(seed, other);
        }

    }
}
