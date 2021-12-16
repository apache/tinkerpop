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

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderLocalStep<S, C extends Comparable> extends ScalarMapStep<S, S> implements ComparatorHolder<S, C>, ByModulating, TraversalParent, Seedable {

    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    private final Random random = new Random();

    public OrderLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void resetSeed(long seed) {
        this.random.setSeed(seed);
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();

        // modulate each traverser object and keep its value to sort on as Pair<traverser-object,List<modulated-object>>
        // as of 3.6.0 this transformation occurs in place and values held in memory so that unproductive by() values
        // can be filtered. without this trade-off for more memory it would be necessary to apply the modulation twice
        // once for the filter and once for the compare.
        if (start instanceof Collection) {
            final Collection<S> original = (Collection<S>) start;
            final List<Pair<S, List<C>>> filteredAndModulated = filterAndModulate(original);
            return (S) filteredAndModulated.stream().map(Pair::getValue0).collect(Collectors.toList());
        } else if (start instanceof Map) {
            final List<Pair<S, List<C>>> filteredAndModulated = filterAndModulate(((Map) start).entrySet());
            final LinkedHashMap sortedMap = new LinkedHashMap();
            filteredAndModulated.stream().map(Pair::getValue0).map(entry -> (Map.Entry) entry).
                    forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
            return (S) sortedMap;
        }

        return start;
    }

    /**
     * Take the collection and apply modulators removing traversers that have modulators that aren't productive.
     */
    private List<Pair<S, List<C>>> filterAndModulate(final Collection<S> original) {
        if (comparators.isEmpty())
            this.comparators.add(new Pair<>(new IdentityTraversal(), (Comparator) Order.asc));

        // detect shuffle and optimize by either ignoring other comparators if shuffle is last or
        // ignoring comparators that are shuffles if they are in the middle
        final boolean isShuffle = (Comparator) this.comparators.get(this.comparators.size() - 1).getValue1() == Order.shuffle;
        final List<Pair<Traversal.Admin<S, C>, Comparator<C>>> relevantComparators = isShuffle ?
                this.comparators :
                this.comparators.stream().filter(p -> (Comparator) p.getValue1() != Order.shuffle).collect(Collectors.toList());

        final List<Pair<S, List<C>>> filteredAndModulated = new ArrayList<>();
        final List<Traversal.Admin<S, C>> modulators = relevantComparators.stream().map(Pair::getValue0).collect(Collectors.toList());
        for (S s : original) {
            // filter out unproductive by()
            final List<C> modulations = modulators.stream().map(t -> TraversalUtil.produce(s, t)).
                    filter(TraversalProduct::isProductive).
                    map(product -> (C) product.get()).collect(Collectors.toList());

            // when sizes arent the same it means a by() wasn't productive and it is ignored
            if (modulations.size() == modulators.size()) {
                filteredAndModulated.add(Pair.with(s, modulations));
            }
        }

        if (isShuffle) {
            Collections.shuffle(filteredAndModulated, random);
        } else {
            // sort the filter/modulated local list in place using the index of the modulator/comparators
            Collections.sort(filteredAndModulated, (o1, o2) -> {
                final List<C> modulated1 = o1.getValue1();
                final List<C> modulated2 = o2.getValue1();
                for (int ix = 0; ix < modulated1.size(); ix++) {
                    final int comparison = relevantComparators.get(ix).getValue1().compare(modulated1.get(ix), modulated2.get(ix));
                    if (comparison != 0)
                        return comparison;
                }

                return 0;
            });
        }

        return filteredAndModulated;
    }

    @Override
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator) {
        this.comparators.add(new Pair<>(this.integrateChild(traversal), comparator));
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal) {
        this.addComparator((Traversal.Admin<S, C>) traversal, (Comparator) Order.asc);
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
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public List<Traversal.Admin<S, C>> getLocalChildren() {
        return (List) this.comparators.stream().map(Pair::getValue0).collect(Collectors.toList());
    }


    @Override
    public OrderLocalStep<S, C> clone() {
        final OrderLocalStep<S, C> clone = (OrderLocalStep<S, C>) super.clone();
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
}
