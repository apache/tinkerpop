package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;

import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackObjectStep<S, V> extends SideEffectStep<S> {

    private final BiFunction<V, S, V> operator;

    public SackObjectStep(final Traversal traversal, final BiFunction<V, S, V> operator) {
        super(traversal);
        this.operator = operator;
        this.setConsumer(traverser -> traverser.sack(this.operator.apply(traverser.sack(), traverser.get())));
    }
}
