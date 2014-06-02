package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.process.util.Reversible;
import com.tinkerpop.gremlin.util.function.SConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends MapStep<S, S> implements Reversible {

    public SideEffectStep(final Traversal traversal, final SConsumer<Traverser<S>> consumer) {
        super(traversal);
        this.setFunction(traverser -> {
            consumer.accept(traverser);
            return traverser.get();
        });
    }

}
