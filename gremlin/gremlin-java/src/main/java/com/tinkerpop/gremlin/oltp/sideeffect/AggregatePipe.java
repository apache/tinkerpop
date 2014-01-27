package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.FlatMapPipe;
import com.tinkerpop.gremlin.util.GremlinHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregatePipe<S> extends FlatMapPipe<S, S> {

    private final String variable;
    private final Function<S, ?>[] preAggregateFunctions;
    private int currentFunction;

    public AggregatePipe(final Pipeline pipeline, final String variable, final Function<S, ?>... preAggregateFunctions) {
        super(pipeline);
        this.variable = variable;
        this.preAggregateFunctions = preAggregateFunctions;
        this.currentFunction = this.preAggregateFunctions.length == 0 ? -1 : 0;
        this.setFunction(holder -> {
            final Collection aggregate = GremlinHelper.getOrCreate(this.pipeline, this.variable, () -> new ArrayList<>());
            final List<S> list = new ArrayList<>();
            if (this.currentFunction == -1) {
                list.add(holder.get());
                aggregate.add(holder.get());
                StreamFactory.stream(this.getPreviousPipe()).forEach(nextHolder -> {
                    list.add(nextHolder.get());
                    aggregate.add(nextHolder.get());
                });
            } else {
                list.add(holder.get());
                aggregate.add(this.preAggregateFunctions[this.currentFunction].apply(holder.get()));
                this.currentFunction = (this.currentFunction + 1) % this.preAggregateFunctions.length;
                StreamFactory.stream(this.getPreviousPipe()).forEach(nextHolder -> {
                    list.add(nextHolder.get());
                    aggregate.add(this.preAggregateFunctions[this.currentFunction].apply(nextHolder.get()));
                    this.currentFunction = (this.currentFunction + 1) % this.preAggregateFunctions.length;
                });
            }
            return list.iterator();
        });
    }
}
