package com.tinkerpop.gremlin.process.marker;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface CapTraversal<S, E> extends Traversal<S, E> {

    public <E2> CapTraversal<S, E2> cap();
}
