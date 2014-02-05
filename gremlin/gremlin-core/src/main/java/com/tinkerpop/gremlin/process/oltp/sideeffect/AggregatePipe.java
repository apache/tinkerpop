package com.tinkerpop.gremlin.process.oltp.sideeffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.oltp.map.FlatMapPipe;
import com.tinkerpop.gremlin.process.oltp.util.FunctionRing;
import com.tinkerpop.gremlin.structure.util.StreamFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregatePipe<S> extends FlatMapPipe<S, S> {

    public final FunctionRing<S, ?> functionRing;
    final Collection aggregate;

    public AggregatePipe(final Traversal pipeline, final String variable, final Function<S, ?>... preAggregateFunctions) {
        super(pipeline);
        this.functionRing = new FunctionRing<>(preAggregateFunctions);
        this.aggregate = this.pipeline.memory().getOrCreate(variable, ArrayList::new);
        this.setFunction(holder -> {
            final List<S> list = new ArrayList<>();
            list.add(holder.get());
            this.aggregate.add(this.functionRing.next().apply(holder.get()));
            StreamFactory.stream(this.getPreviousPipe()).forEach(nextHolder -> {
                list.add(nextHolder.get());
                this.aggregate.add(this.functionRing.next().apply(nextHolder.get()));
            });
            return list.iterator();
        });
    }
}
