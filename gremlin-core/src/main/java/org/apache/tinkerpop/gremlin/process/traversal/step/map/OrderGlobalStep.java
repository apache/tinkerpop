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
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Order;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderGlobalStep<S> extends CollectingBarrierStep<S> implements ComparatorHolder<S>, TraversalParent {

    private final List<Comparator<S>> comparators = new ArrayList<>();

    public OrderGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected void barrierConsumer(final TraverserSet<S> traverserSet) {
        traverserSet.sort(this.comparators.isEmpty() ? new ComparatorTraverser(Order.incr) : new ChainedComparator(ComparatorTraverser.convertComparator((List) this.comparators)));
    }

    @Override
    public void addComparator(final Comparator<S> comparator) {
        if (comparator instanceof TraversalComparator)
            this.integrateChild(((TraversalComparator) comparator).getTraversal());
        this.comparators.add(comparator);
    }

    @Override
    public List<Comparator<S>> getComparators() {
        return this.comparators.isEmpty() ? Collections.singletonList((Comparator) Order.incr) : Collections.unmodifiableList(this.comparators);
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.comparators);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.unmodifiableList(this.comparators.stream()
                .filter(comparator -> comparator instanceof TraversalComparator)
                .map(traversalComparator -> ((TraversalComparator<S, E>) traversalComparator).getTraversal())
                .collect(Collectors.toList()));
    }

    @Override
    public void addLocalChild(final Traversal.Admin<?, ?> localChildTraversal) {
        throw new UnsupportedOperationException("Use OrderGlobalStep.addComparator(" + TraversalComparator.class.getSimpleName() + ") to add a local child traversal:" + this);
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

        public static <S> List<ComparatorTraverser<S>> convertComparator(final List<Comparator<S>> comparators) {
            return comparators.stream().map(comparator -> new ComparatorTraverser<>(comparator)).collect(Collectors.toList());
        }
    }
}
