package com.tinkerpop.gremlin.pipes.map;

import com.tinkerpop.gremlin.Gremlin;
import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.pipes.AbstractPipe;

import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class MapPipe<S, E> extends AbstractPipe<S, E> {

    private Function<Holder<S>, E> function;

    public MapPipe(final Gremlin pipeline) {
        super(pipeline);
    }

    public MapPipe(final Gremlin pipeline, final Function<Holder<S>, E> function) {
        super(pipeline);
        this.function = function;
    }

    protected Holder<E> processNextStart() {
        while (true) {
            final Holder<S> holder = this.starts.next();
            final E temp = this.function.apply(holder);
            if (NO_OBJECT != temp)
                if (holder.get().equals(temp)) // no path extension (i.e. a filter, identity, side-effect)
                    return (Holder<E>) holder.makeSibling();
                else
                    return holder.makeChild(this.getAs(), temp);
        }
    }

    public void setFunction(final Function<Holder<S>, E> function) {
        this.function = function;
    }
}

