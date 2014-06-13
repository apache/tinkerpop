package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.UnBulkable;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateStep<S> extends FlatMapStep<S, S> implements Reversible, UnBulkable {

    public final FunctionRing<S, ?> functionRing;
    final Collection aggregate;

    public AggregateStep(final Traversal traversal, final String variable, final SFunction<S, ?>... preAggregateFunctions) {
        super(traversal);
        this.functionRing = new FunctionRing<>(preAggregateFunctions);
        this.aggregate = this.traversal.memory().getOrCreate(variable, ArrayList::new);
        this.setFunction(traverser -> {
            final List<S> list = new ArrayList<>();
            list.add(traverser.get());
            this.aggregate.add(this.functionRing.next().apply(traverser.get()));
            StreamFactory.stream(this.getPreviousStep()).forEach(nextHolder -> {
                list.add(nextHolder.get());
                this.aggregate.add(this.functionRing.next().apply(nextHolder.get()));
            });
            return list.iterator();
        });
    }
}
