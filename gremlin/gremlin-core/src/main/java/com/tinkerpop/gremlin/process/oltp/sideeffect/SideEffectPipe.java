package com.tinkerpop.gremlin.process.oltp.sideeffect;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.map.MapPipe;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectPipe<S> extends MapPipe<S, S> {

    public SideEffectPipe(final Traversal pipeline, final Consumer<Holder<S>> consumer) {
        super(pipeline);
        this.setFunction(holder -> {
            consumer.accept(holder);
            return holder.get();
        });
    }

}
