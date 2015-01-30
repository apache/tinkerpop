package com.tinkerpop.gremlin.process.graph.traversal.step.filter;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.TraversalHolder;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrStep<S> extends ConjunctionStep<S> implements TraversalHolder {

    public OrStep(final Traversal traversal, final Traversal<S, ?>... orTraversals) {
        super(traversal, orTraversals);

    }
}
