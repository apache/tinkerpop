package com.tinkerpop.gremlin.process.graph.marker;

import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComparatorHolder<S> {

    public void addComparator(final Comparator<S> comparator);

    public List<Comparator<S>> getComparators();

}
