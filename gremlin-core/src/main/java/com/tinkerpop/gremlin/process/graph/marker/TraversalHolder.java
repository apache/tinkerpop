package com.tinkerpop.gremlin.process.graph.marker;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.Collection;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface TraversalHolder<S, E> extends PathConsumer {

    public Collection<Traversal<S, E>> getTraversals();

    public default boolean requiresPaths() {
        return this.getTraversals().stream().filter(TraversalHelper::trackPaths).findAny().isPresent();
    }

}
