package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MarkerIdentityStep<S> extends SideEffectStep<S> {

    public MarkerIdentityStep(final Traversal traversal) {
        super(traversal);
    }
}
