package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IdentityStep<S> extends SideEffectStep<S> {

    public IdentityStep(final Traversal traversal) {
        super(traversal);
    }
}
