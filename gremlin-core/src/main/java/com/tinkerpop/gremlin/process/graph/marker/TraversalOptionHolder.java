package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalOptionHolder<M,S,E> extends TraversalHolder {

    public static enum Pick { any, none }

    public void addOption(final M pickToken, final Traversal<S, E> traversalOption);
}
