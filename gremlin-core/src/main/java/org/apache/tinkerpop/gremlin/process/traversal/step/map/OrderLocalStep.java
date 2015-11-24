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
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.TraversalComparator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

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
public final class OrderLocalStep<S, M> extends MapStep<S, S> implements ComparatorHolder<M>, TraversalParent {

    private List<Comparator<M>> comparators = new ArrayList<>();
    private Comparator<M> chainedComparator = null;

    public OrderLocalStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected S map(final Traverser.Admin<S> traverser) {
        final S start = traverser.get();
        if (start instanceof Collection)
            return (S) OrderLocalStep.sortCollection((Collection) start, this.chainedComparator);
        else if (start instanceof Map)
            return (S) OrderLocalStep.sortMap((Map) start, this.chainedComparator);
        else
            return start;
    }

    @Override
    public void addComparator(final Comparator<M> comparator) {
        this.comparators.add(comparator);
        if (comparator instanceof TraversalComparator)
            this.integrateChild(((TraversalComparator) comparator).getTraversal());
        this.chainedComparator = this.comparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
    }

    @Override
    public List<Comparator<M>> getComparators() {
        return Collections.unmodifiableList(this.comparators);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.comparators);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        for (final Comparator<M> comparator : this.comparators) {
            result ^= comparator.hashCode();
        }
        return result;
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
        this.addComparator(new TraversalComparator<>((Traversal.Admin) localChildTraversal, Order.incr));
    }

    @Override
    public OrderLocalStep<S,M> clone() {
        final OrderLocalStep<S,M> clone = (OrderLocalStep<S,M>) super.clone();
        clone.comparators = new ArrayList<>();
        for(final Comparator<M> comparator : this.comparators) {
            clone.addComparator(comparator instanceof TraversalComparator ? ((TraversalComparator) comparator).clone() : comparator);
        }
        return clone;
    }

    /////////////

    private static final <A> List<A> sortCollection(final Collection<A> collection, final Comparator<?> comparator) {
        if (collection instanceof List) {
            Collections.sort((List) collection, (Comparator) comparator);
            return (List<A>) collection;
        } else {
            final List<A> list = new ArrayList<>(collection);
            Collections.sort(list, (Comparator) comparator);
            return list;
        }
    }

    private static final <K, V> Map<K, V> sortMap(final Map<K, V> map, final Comparator<?> comparator) {
        final List<Map.Entry<K, V>> entries = new ArrayList<>(map.entrySet());
        Collections.sort(entries, (Comparator) comparator);
        final LinkedHashMap<K, V> sortedMap = new LinkedHashMap<>();
        entries.forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
        return sortedMap;
    }
}
