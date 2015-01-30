package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Order;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrderLocalStep<S, M> extends MapStep<S, S> implements Reversible, ComparatorHolder<M> {

    private final List<Comparator<M>> comparators = new ArrayList<>();

    public OrderLocalStep(final Traversal traversal) {
        super(traversal);
        super.setFunction(Traverser::get);
        ///
        this.setFunction(traverser -> {
            final Object object = traverser.get();
            if (object instanceof Collection)
                return (S) OrderLocalStep.sortCollection((List) object, Order.incr);
            else if (object instanceof Map)
                return (S) OrderLocalStep.sortMap((Map) object, Order.valueIncr);
            else
                throw new IllegalArgumentException("The provided object can not be ordered: " + object);
        });
    }

    @Override
    public void addComparator(final Comparator<M> comparator) {
        this.comparators.add(comparator);
        final Comparator<M> chainedComparator = this.comparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
        ///
        this.setFunction(traverser -> {
            final Object object = traverser.get();
            if (object instanceof Collection)
                return (S) OrderLocalStep.sortCollection((List) object, chainedComparator);
            else if (object instanceof Map)
                return (S) OrderLocalStep.sortMap((Map) object, chainedComparator);
            else
                throw new IllegalArgumentException("The provided object can not be ordered: " + object);
        });
    }

    @Override
    public List<Comparator<M>> getComparators() {
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
