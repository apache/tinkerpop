package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.TraversalParent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndStep<S> extends ConjunctionStep<S> implements TraversalParent {

    public AndStep(final Traversal traversal, final Traversal.Admin<S, ?>... andTraversals) {
        super(traversal, andTraversals);

    }
}
