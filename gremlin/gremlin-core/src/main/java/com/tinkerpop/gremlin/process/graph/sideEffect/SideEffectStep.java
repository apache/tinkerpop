package com.tinkerpop.gremlin.process.graph.sideEffect;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.map.MapStep;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectStep<S> extends MapStep<S, S> {

    public SideEffectStep(final Traversal traversal, final Consumer<Holder<S>> consumer) {
        super(traversal);
        this.setFunction(holder -> {
            consumer.accept(holder);
            return holder.get();
        });
    }

}
