package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackStep<S, E> extends MapStep<S, E> {

    public SackStep(final Traversal traversal) {
        super(traversal);
        this.setFunction(traverser -> traverser.sack());
    }
}
