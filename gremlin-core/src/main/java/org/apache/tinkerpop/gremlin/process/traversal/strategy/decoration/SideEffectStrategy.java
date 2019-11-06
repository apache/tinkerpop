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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;
import org.javatuples.Triplet;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SideEffectStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final List<Triplet<String, Supplier, BinaryOperator>> sideEffects = new ArrayList<>();

    private SideEffectStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.isRoot()) {
            this.sideEffects.forEach(triplet -> traversal.getSideEffects().register(triplet.getValue0(), triplet.getValue1(), triplet.getValue2()));
        }
    }

    public static <A> void addSideEffect(final TraversalStrategies traversalStrategies, final String key, final A value, final BinaryOperator<A> reducer) {
        SideEffectStrategy strategy = (SideEffectStrategy) traversalStrategies.toList().stream().filter(s -> s instanceof SideEffectStrategy).findAny().orElse(null);
        if (null == strategy) {
            strategy = new SideEffectStrategy();
            traversalStrategies.addStrategies(strategy);
        } else {
            final SideEffectStrategy cloneStrategy = new SideEffectStrategy();
            cloneStrategy.sideEffects.addAll(strategy.sideEffects);
            strategy = cloneStrategy;
            traversalStrategies.addStrategies(strategy);
        }
        strategy.sideEffects.add(new Triplet<>(key, null == value ? null : value instanceof Supplier ? (Supplier) value : new ConstantSupplier<>(value), reducer));
    }
}