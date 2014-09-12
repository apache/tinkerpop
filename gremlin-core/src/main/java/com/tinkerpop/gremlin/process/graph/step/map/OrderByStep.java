package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.UnTraverserIterator;
import com.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrderByStep<S extends Element, C extends Comparable> extends FlatMapStep<S, S> implements Reversible {

    public Comparator comparator;
    public final String key;

    public OrderByStep(final Traversal traversal, final String key, final Comparator comparator) {
        super(traversal);
        this.key = key;
        this.comparator = comparator;
        this.setFunction(traverser -> {
            final SortedMap<C, List<Traverser<S>>> sortedMap = new TreeMap<>((Comparator<C>) this.comparator);
            final List<Traverser<S>> list = new ArrayList<>();
            list.add(traverser);
            sortedMap.put(traverser.get().value(this.key), list);

            this.starts.forEachRemaining(t -> {
                List<Traverser<S>> newList = sortedMap.get(t.get().<C>value(this.key));
                if (null == newList) {
                    newList = new ArrayList<>();
                    sortedMap.put(t.get().value(this.key), newList);
                }
                newList.add(t);
            });
            return new UnTraverserIterator<>(sortedMap.values().stream().flatMap(Collection::stream).iterator());
        });
    }
}
