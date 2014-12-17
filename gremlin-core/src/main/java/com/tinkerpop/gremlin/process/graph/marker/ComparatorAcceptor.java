package com.tinkerpop.gremlin.process.graph.marker;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComparatorAcceptor<S> {

    public void addComparator(final Comparator<S> comparator);

}
