package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.structure.Element;

import java.util.Comparator;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComparatorAcceptor<S> {

    public void addComparator(final Comparator<S> comparator);

    public default void addComparator(final String key, final Comparator<S> comparator) {
        this.addComparator((a, b) -> comparator.compare(((Element) a).value(key), ((Element) b).value(key)));
    }

    public default void addComparator(final Function<Element, S> function, final Comparator<S> comparator) {
        this.addComparator((a, b) -> comparator.compare(function.apply((Element) a), function.apply((Element) b)));
    }
}
