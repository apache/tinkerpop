package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class PathIdentityStep<S> extends SideEffectStep<S> implements PathConsumer {

    public PathIdentityStep(final Traversal traversal) {
        super(traversal);
    }
}
