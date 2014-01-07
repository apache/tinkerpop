package com.tinkerpop.gremlin.pipes;

import com.tinkerpop.gremlin.pipes.util.Holder;
import com.tinkerpop.gremlin.pipes.util.PipelineHelper;

import java.util.Optional;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapPipe<S, E> extends AbstractPipe<S, E> {

    private final Function<Holder<S>, E> function;

    public MapPipe(final Pipeline pipeline, final Function<Holder<S>, E> function) {
        super(pipeline);
        this.function = function;
    }

    public Holder<E> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            final E temp = this.function.apply(holder);
            holder.setPipe(PipelineHelper.getNextPipeLabel(this.pipeline, this).orElse("NONE"));

            if (Pipe.NO_OBJECT != temp)
                if (holder.get().equals(temp))
                    return (Holder<E>) holder.makeSibling(this.getAs()); // no path extension
                else
                    return holder.makeChild(this.getAs(), temp);
        }
    }
}
