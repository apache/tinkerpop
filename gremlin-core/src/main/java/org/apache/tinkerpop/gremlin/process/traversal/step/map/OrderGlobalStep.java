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

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderGlobalStep<S, C extends Comparable> extends CollectingBarrierStep<S> implements ComparatorHolder<S, C>, TraversalParent, ByModulating {

    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    private ChainedComparator<S, C> chainedComparator = null;
    private long limit = Long.MAX_VALUE;

    public OrderGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        if (null == this.chainedComparator)
            this.chainedComparator = new ChainedComparator<>(true, this.comparators);
        if (this.chainedComparator.isShuffle())
            traverserSet.shuffle();
        else
            traverserSet.sort((Comparator) this.chainedComparator);
    }

    public void setLimit(final long limit) {
        this.limit = limit;
    }

    public long getLimit() {
        return this.limit;
    }

    @Override
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator) {
        this.comparators.add(new Pair<>(this.integrateChild(traversal), comparator));
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal) {
        this.modulateBy(traversal, Order.incr);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal, final Comparator comparator) {
        this.addComparator((Traversal.Admin<S, C>) traversal, comparator);
    }

    @Override
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators() {
        return this.comparators.isEmpty() ? Collections.singletonList(new Pair<>(new IdentityTraversal(), (Comparator) Order.incr)) : Collections.unmodifiableList(this.comparators);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.comparators);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (int i = 0; i < this.comparators.size(); i++) {
            result ^= this.comparators.get(i).hashCode() * (i + 1);
        }
        return result;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.BULK, TraverserRequirement.OBJECT);
    }

    @Override
    public List<Traversal.Admin<S, C>> getLocalChildren() {
        return (List) this.comparators.stream().map(Pair::getValue0).collect(Collectors.toList());
    }

    @Override
    public OrderGlobalStep<S, C> clone() {
        final OrderGlobalStep<S, C> clone = (OrderGlobalStep<S, C>) super.clone();
        clone.comparators = new ArrayList<>();
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> comparator : this.comparators) {
            clone.comparators.add(new Pair<>(comparator.getValue0().clone(), comparator.getValue1()));
        }
        clone.chainedComparator = null;
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.comparators.stream().map(Pair::getValue0).forEach(TraversalParent.super::integrateChild);
    }

    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        if (null == this.chainedComparator)
            this.chainedComparator = new ChainedComparator<>(true, this.comparators);
        return MemoryComputeKey.of(this.getId(), new OrderBiOperator<>(this.chainedComparator, this.limit), false, true);
    }

    ////////////////

    public static final class OrderBiOperator<S> implements BinaryOperator<TraverserSet<S>>, Serializable, Cloneable {

        private ChainedComparator chainedComparator;
        private long limit;

        private OrderBiOperator() {
            // for serializers that need a no-arg constructor
        }

        public OrderBiOperator(final ChainedComparator<S, ?> chainedComparator, final long limit) {
            this.chainedComparator = chainedComparator;
            this.limit = limit;
        }

        @Override
        public OrderBiOperator<S> clone() {
            try {
                final OrderBiOperator<S> clone = (OrderBiOperator<S>) super.clone();
                clone.chainedComparator = this.chainedComparator.clone();
                return clone;
            } catch (final CloneNotSupportedException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public TraverserSet<S> apply(final TraverserSet<S> setA, final TraverserSet<S> setB) {
            setA.addAll(setB);
            if (this.limit != -1 && setA.bulkSize() > this.limit) {
                if (this.chainedComparator.isShuffle())
                    setA.shuffle();
                else
                    setA.sort(this.chainedComparator);
                long counter = 0l;
                final Iterator<Traverser.Admin<S>> traversers = setA.iterator();
                while (traversers.hasNext()) {
                    final Traverser.Admin<S> traverser = traversers.next();
                    if (counter > this.limit)
                        traversers.remove();
                    counter = counter + traverser.bulk();
                }
            }
            return setA;
        }
    }
}
