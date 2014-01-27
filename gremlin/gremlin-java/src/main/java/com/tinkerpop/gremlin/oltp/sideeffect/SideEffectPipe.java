package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.MapPipe;

import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SideEffectPipe<S> extends MapPipe<S, S> {

    public SideEffectPipe(final Pipeline pipeline, final Consumer<Holder<S>> consumer) {
        super(pipeline);
        this.setFunction(holder -> {
            consumer.accept(holder);
            return holder.get();
        });
    }

}
