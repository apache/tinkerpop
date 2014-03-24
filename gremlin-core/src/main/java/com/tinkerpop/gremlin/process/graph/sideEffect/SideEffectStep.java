package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;
import com.tinkerpop.gremlin.util.function.SConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends MapStep<S, S> {

    public SideEffectStep(final Traversal traversal, final SConsumer<Holder<S>> consumer) {
        super(traversal);
        this.setFunction(holder -> {
            consumer.accept(holder);
            return holder.get();
        });
    }

}
