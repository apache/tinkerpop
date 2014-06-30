package com.tinkerpop.gremlin.process;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalEngine {

    public <E> Iterator<E> execute(final Traversal<?, E> traversal);
}
