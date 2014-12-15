package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.util.ByComparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface ByComparatorAcceptor<S> {

    public void setByComparator(final ByComparator<S> byComparator);
}
