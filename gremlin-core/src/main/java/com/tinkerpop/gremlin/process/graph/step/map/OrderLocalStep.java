package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.ComparatorHolder;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.ArrayList;
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
    }

    @Override
    public void addComparator(final Comparator<M> comparator) {
        this.comparators.add(comparator);
        final Comparator<M> chainedComparator = this.comparators.stream().reduce((a, b) -> a.thenComparing(b)).get();
        ///
        this.setFunction(traverser -> {
            final Object object = traverser.get();
            if (object instanceof List) {
                Collections.sort((List) object, chainedComparator);
                return (S) object;
            } else if (object instanceof Map) {
                final List<Map.Entry<?, ?>> entries = new ArrayList<>(((Map<?, ?>) object).entrySet());
                Collections.sort(entries, (Comparator<Map.Entry<?, ?>>) chainedComparator);
                final LinkedHashMap<Object, Object> sortedMap = new LinkedHashMap<>();
                entries.forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
                return (S) sortedMap;
            } else {
                throw new IllegalArgumentException("The provided object can not be ordered: " + object);
            }
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
}
