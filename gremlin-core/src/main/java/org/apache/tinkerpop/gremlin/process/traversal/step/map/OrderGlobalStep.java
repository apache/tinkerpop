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
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.MultiComparator;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderGlobalStep<S, C extends Comparable> extends CollectingBarrierStep<S> implements ComparatorHolder<S, C>, TraversalParent, ByModulating, Seedable {

    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    private MultiComparator<C> multiComparator = null;
    private long limit = Long.MAX_VALUE;
    private final Random random = new Random();

    public OrderGlobalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void resetSeed(long seed) {
        this.random.setSeed(seed);
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        if (null == this.multiComparator) this.multiComparator = this.createMultiComparator();
        //
        if (this.multiComparator.isShuffle())
            traverserSet.shuffle(random);
        else
            traverserSet.sort((Comparator) this.multiComparator);
    }

    @Override
    public void processAllStarts() {
        while (this.starts.hasNext()) {
            // only add the traverser if the comparator traversal was productive
            this.createProjectedTraverser(this.starts.next()).ifPresent(traverserSet::add);
        }
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
        this.modulateBy(traversal, Order.asc);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal, final Comparator comparator) {
        this.addComparator((Traversal.Admin<S, C>) traversal, comparator);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        int i = 0;
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> pair : this.comparators) {
            final Traversal.Admin<S, C> traversal = pair.getValue0();
            if (null != traversal && traversal.equals(oldTraversal)) {
                this.comparators.set(i, Pair.with(this.integrateChild(newTraversal), pair.getValue1()));
                break;
            }
            i++;
        }
    }

    @Override
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators() {
        return this.comparators.isEmpty() ? Collections.singletonList(new Pair<>(new IdentityTraversal(), (Comparator) Order.asc)) : Collections.unmodifiableList(this.comparators);
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
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.comparators.stream().map(Pair::getValue0).forEach(TraversalParent.super::integrateChild);
    }

    @Override
    public MemoryComputeKey<TraverserSet<S>> getMemoryComputeKey() {
        if (null == this.multiComparator) this.multiComparator = this.createMultiComparator();
        return MemoryComputeKey.of(this.getId(), new OrderBiOperator<>(this.limit, this.multiComparator, this.random), false, true);
    }

    private Optional<ProjectedTraverser<S, Object>> createProjectedTraverser(final Traverser.Admin<S> traverser) {
        // this was ProjectedTraverser<S, C> but the projection may not be C in the case of a lambda where a
        // Comparable may not be expected but rather an object that can be compared in any way given a lambda.
        // not sure why this is suddenly an issue but Intellij would not let certain tests pass without this
        // adjustment here.
        final List<Object> projections = new ArrayList<>(this.comparators.size());
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> pair : this.comparators) {
            final TraversalProduct product = TraversalUtil.produce(traverser, pair.getValue0());
            if (!product.isProductive()) break;
            projections.add(product.get());
        }

        // if a traversal wasn't productive then the sizes wont match and it will filter
        return projections.size() == comparators.size() ? Optional.of(new ProjectedTraverser(traverser, projections)) : Optional.empty();
    }

    private final MultiComparator<C> createMultiComparator() {
        final List<Comparator<C>> list = new ArrayList<>(this.comparators.size());
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> pair : this.comparators) {
            list.add(pair.getValue1());
        }
        return new MultiComparator<>(list);
    }

    ////////////////

    public static final class OrderBiOperator<S> implements BinaryOperator<TraverserSet<S>>, Serializable {

        private long limit;
        private MultiComparator comparator;
        private Random random;

        private OrderBiOperator() {
            // for serializers that need a no-arg constructor
        }

        public OrderBiOperator(final long limit, final MultiComparator multiComparator, final Random random) {
            this.limit = limit;
            this.comparator = multiComparator;
            this.random = random;
        }

        @Override
        public TraverserSet<S> apply(final TraverserSet<S> setA, final TraverserSet<S> setB) {
            setA.addAll(setB);
            if (this.limit != -1 && setA.bulkSize() > this.limit) {
                if (this.comparator.isShuffle())
                    setA.shuffle(random);
                else
                    setA.sort(this.comparator);
                long counter = 0L;
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
