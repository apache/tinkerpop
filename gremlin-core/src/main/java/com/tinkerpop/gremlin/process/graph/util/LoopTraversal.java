package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.traversal.lambda.AbstractSingleTraversal;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LoopTraversal<S, E> extends AbstractSingleTraversal<S, E> {

    private long maxLoops;
    private boolean allow = false;

    public LoopTraversal(final long maxLoops) {
        this.maxLoops = maxLoops;
    }

    @Override
    public boolean hasNext() {
        return this.allow;
    }

    @Override
    public void addStart(final Traverser<S> start) {
        this.allow = start.loops() >= this.maxLoops;
    }

    @Override
    public String toString() {
        return "loops(" + this.maxLoops + ")";
    }
}
