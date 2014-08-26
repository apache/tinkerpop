package com.tinkerpop.gremlin.process.graph.step.util;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LabelIdentityStep<S> extends SideEffectStep<S> {

    public LabelIdentityStep(final Traversal traversal) {
        super(traversal);
    }
}
