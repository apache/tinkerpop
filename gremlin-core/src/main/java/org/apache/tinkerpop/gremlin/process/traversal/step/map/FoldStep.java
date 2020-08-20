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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.util.function.ArrayListSupplier;

import java.io.Serializable;
import java.util.ArrayList;
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
    private final boolean listFold;

    public FoldStep(final Traversal.Admin traversal) {
        this(traversal, (Supplier) ArrayListSupplier.instance(), (BiFunction) Operator.addAll);
    }

    public FoldStep(final Traversal.Admin traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.listFold = Operator.addAll.equals(foldFunction);
        this.setSeedSupplier(seed);
        this.setReducingBiOperator(new FoldBiOperator<>(foldFunction));
    }

    @Override
    public E projectTraverser(final Traverser.Admin<S> traverser) {
        if (this.listFold) {
            final List<S> list = new ArrayList<>();
            for (long i = 0; i < traverser.bulk(); i++) {
                list.add(traverser.get());
            }
            return (E) list;
        } else {
            return (E) traverser.get();
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    public boolean isListFold() {
        return listFold;
    }

    public static class FoldBiOperator<E> implements BinaryOperator<E>, Serializable {

        private BiFunction biFunction;

        private FoldBiOperator() {
            // for serialization purposes
        }

        public FoldBiOperator(final BiFunction biFunction) {
            this.biFunction = biFunction;
        }

        @Override
        public E apply(E seed, E other) {
            return (E) this.biFunction.apply(seed, other);
        }

    }
}
