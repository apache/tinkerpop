package com.tinkerpop.gremlin.oltp.sideeffect;

import com.tinkerpop.blueprints.util.StreamFactory;
import com.tinkerpop.gremlin.Pipeline;
import com.tinkerpop.gremlin.oltp.map.FlatMapPipe;
import com.tinkerpop.gremlin.util.FunctionRing;
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
    public final FunctionRing<S, ?> functionRing;

    public AggregatePipe(final Pipeline pipeline, final String variable, final Function<S, ?>... preAggregateFunctions) {
        super(pipeline);
        this.variable = variable;
        this.functionRing = new FunctionRing<>(preAggregateFunctions);
        this.setFunction(holder -> {
            final Collection aggregate = GremlinHelper.getOrCreate(this.pipeline, this.variable, () -> new ArrayList<>());
            final List<S> list = new ArrayList<>();
            list.add(holder.get());
            aggregate.add(this.functionRing.next().apply(holder.get()));
            StreamFactory.stream(this.getPreviousPipe()).forEach(nextHolder -> {
                list.add(nextHolder.get());
                aggregate.add(this.functionRing.next().apply(nextHolder.get()));
            });
            return list.iterator();
        });
    }
}
