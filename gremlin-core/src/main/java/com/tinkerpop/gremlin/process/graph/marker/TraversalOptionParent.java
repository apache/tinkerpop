package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.TraversalParent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalOptionParent<M, S, E> extends TraversalParent {

    public static enum Pick {any, none}

    public void addGlobalChildOption(final M pickToken, final Traversal.Admin<S, E> traversalOption);
}
