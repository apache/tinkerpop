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
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.ChainedComparator;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderLocalStep<S, C extends Comparable> extends MapStep<S, S> implements ComparatorHolder<S, C>, ByModulating, TraversalParent {

    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    private ChainedComparator<S, C> chainedComparator = null;

    public OrderLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        if (null == this.chainedComparator)
            this.chainedComparator = new ChainedComparator<>(false, this.comparators);
        final S start = traverser.get();
        if (start instanceof Collection)
            return (S) OrderLocalStep.sortCollection((Collection) start, this.chainedComparator);
        else if (start instanceof Map)
            return (S) OrderLocalStep.sortMap((Map) start, this.chainedComparator);
        else
            return start;
    }

    @Override
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator) {
        this.comparators.add(new Pair<>(this.integrateChild(traversal), comparator));
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal) {
        this.addComparator((Traversal.Admin<S, C>) traversal, (Comparator) Order.incr);
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
        clone.chainedComparator = null;
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.comparators.stream().map(Pair::getValue0).forEach(TraversalParent.super::integrateChild);
    }

    /////////////

    private static final <A> List<A> sortCollection(final Collection<A> collection, final ChainedComparator comparator) {
        if (collection instanceof List) {
            if (comparator.isShuffle())
                Collections.shuffle((List) collection);
            else
                Collections.sort((List) collection, comparator);
            return (List<A>) collection;
        } else {
            return sortCollection(new ArrayList<>(collection), comparator);
        }
    }

    private static final <K, V> Map<K, V> sortMap(final Map<K, V> map, final ChainedComparator comparator) {
        final List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        if (comparator.isShuffle())
            Collections.shuffle(entries);
        else
            Collections.sort(entries, comparator);
        final LinkedHashMap<K, V> sortedMap = new LinkedHashMap<>();
        entries.forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
        return sortedMap;
    }
}
