package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Comparing<S> {

    public Comparator<Traverser<S>> getComparator();
}
