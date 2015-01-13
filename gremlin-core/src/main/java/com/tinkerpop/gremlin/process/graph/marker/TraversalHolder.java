package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalHolder<S, E> {

    public Collection<Traversal<S, E>> getTraversals();

}
