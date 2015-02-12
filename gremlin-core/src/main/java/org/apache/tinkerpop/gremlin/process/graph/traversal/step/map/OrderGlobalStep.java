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
import com.tinkerpop.gremlin.process.graph.traversal.step.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.CollectingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.step.Reversible;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraverserSet;
import com.tinkerpop.gremlin.structure.Order;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderGlobalStep<S> extends CollectingBarrierStep<S> implements Reversible, ComparatorHolder<S> {

    private final List<Comparator<S>> comparators = new ArrayList<>();
    private Comparator<Traverser<S>> chainedComparator = (Comparator) Order.incr;

    public OrderGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        traverserSet.sort(this.chainedComparator);
    }

    @Override
    public void addComparator(final Comparator<S> comparator) {
        this.comparators.add(comparator);
        this.chainedComparator = this.comparators.stream().map(c -> (Comparator<Traverser<S>>) new ComparatorTraverser<S>(c)).reduce((a, b) -> a.thenComparing(b)).get();
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return this.comparators;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    /////

    private static class ComparatorTraverser<S> implements Comparator<Traverser<S>>, Serializable {

        private final Comparator<S> comparator;

        public ComparatorTraverser(final Comparator<S> comparator) {
            this.comparator = comparator;
        }

        @Override
        public int compare(final Traverser<S> traverserA, final Traverser<S> traverserB) {
            return this.comparator.compare(traverserA.get(), traverserB.get());
        }
    }
}
