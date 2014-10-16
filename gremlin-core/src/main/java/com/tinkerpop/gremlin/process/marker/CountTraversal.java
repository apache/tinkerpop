package com.tinkerpop.gremlin.process.marker;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface CountTraversal<S, E> extends Traversal<S,E> {

    public CountTraversal<S, Long> count();
}
