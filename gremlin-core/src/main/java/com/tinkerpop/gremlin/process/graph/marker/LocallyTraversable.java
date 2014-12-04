package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface LocallyTraversable<E> {

    public void setLocalTraversal(final Traversal<E, E> localTraversal);

    public Traversal<E, E> getLocalTraversal();
}
