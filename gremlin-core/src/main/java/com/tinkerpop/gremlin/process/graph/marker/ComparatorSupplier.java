package com.tinkerpop.gremlin.process.graph.marker;

import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ComparatorSupplier<S> {

    public List<Comparator<S>> getComparators();
}
