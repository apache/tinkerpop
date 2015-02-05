package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.TraversalParent;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndStep<S> extends ConjunctionStep<S> implements TraversalParent {

    public AndStep(final Traversal.Admin traversal, final Traversal.Admin<S, ?>... andTraversals) {
        super(traversal, andTraversals);

    }

    public static final class AndMarker<S> extends ConjunctionMarker<S> {
        public AndMarker(final Traversal.Admin traversal) {
            super(traversal);
        }
    }
}
