package com.tinkerpop.gremlin.process.graph.util;

import com.tinkerpop.gremlin.process.Traverser;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LoopPredicate<S> implements Predicate<Traverser<S>> {
    private final int maxLoops;

    public LoopPredicate(final int maxLoops) {
        this.maxLoops = maxLoops;
    }

    @Override
    public boolean test(final Traverser<S> traverser) {
        return traverser.loops() >= this.maxLoops;
    }

    @Override
    public String toString() {
        return "loops(" + this.maxLoops + ")";
    }
}