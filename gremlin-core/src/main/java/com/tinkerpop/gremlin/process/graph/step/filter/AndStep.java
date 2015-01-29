package com.tinkerpop.gremlin.process.graph.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndStep<S> extends ConjunctionStep<S> implements TraversalHolder {

    public AndStep(final Traversal traversal, final Traversal<S, ?>... andTraversals) {
        super(traversal, andTraversals);

    }
}
