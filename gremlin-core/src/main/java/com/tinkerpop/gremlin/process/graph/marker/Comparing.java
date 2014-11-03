package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.Comparator;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Comparing<S> {

    public Comparator<Traverser<S>>[] getComparators();
}
