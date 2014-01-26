package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.gremlin.Holder;
import com.tinkerpop.gremlin.Pipe;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.AbstractPipe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregatePipe<S, T> extends AbstractPipe<S, S> {

    private Iterator<Holder<S>> itty = null;
    private final String variable;
    private final Function<S, ?>[] preAggregateFunctions;

    public AggregatePipe(final Pipeline pipeline, final String variable, final Function<S, ?>... preAggregateFunctions) {
        super(pipeline);
        this.variable = variable;
        this.preAggregateFunctions = preAggregateFunctions;
    }

    protected Holder<S> processNextStart() {
        if (null != this.itty) {
            return this.itty.next();
        } else {
            final List<Holder<S>> toReturn = new ArrayList<>();

            final Collection collection = (Collection<T>) this.pipeline.get(this.variable).orElseGet(() -> {
                final List list = new ArrayList<>();
                this.pipeline.set(this.variable, list);
                return list;
            });

            try {
                final Pipe<?, S> pipe = this.getPreviousPipe();
                int currentFunction = this.preAggregateFunctions.length == 0 ? -1 : 0;
                while (true) {
                    final Holder<S> holder = pipe.next();
                    toReturn.add(holder);
                    collection.add(currentFunction == -1 ? holder.get() : this.preAggregateFunctions[currentFunction].apply(holder.get()));
                    if (currentFunction != -1)
                        currentFunction = (currentFunction + 1) % this.preAggregateFunctions.length;
                }
            } catch (final NoSuchElementException e) {

            }

            this.itty = toReturn.iterator();
            return this.processNextStart();
        }
    }
}
