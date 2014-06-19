package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.step.map.FlatMapStep;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.StreamFactory;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateStep<S> extends FlatMapStep<S, S> implements Reversible, Bulkable {

    public final FunctionRing<S, ?> functionRing;
    final Collection aggregate;
    private long bulkCount = 1l;

    public String variable;


    public AggregateStep(final Traversal traversal, final String variable, final SFunction<S, ?>... preAggregateFunctions) {
        super(traversal);
        this.variable = variable;
        this.functionRing = new FunctionRing<>(preAggregateFunctions);
        this.aggregate = this.traversal.memory().getOrCreate(this.variable, ArrayList::new);
        this.setFunction(traverser -> {
            final List<S> list = new ArrayList<>();
            for (int i = 0; i < this.bulkCount; i++) {
                list.add(traverser.get());
                this.aggregate.add(this.functionRing.next().apply(traverser.get()));
            }
            StreamFactory.stream(this.getPreviousStep()).forEach(nextHolder -> {
                for (int i = 0; i < this.bulkCount; i++) {
                    list.add(nextHolder.get());
                    this.aggregate.add(this.functionRing.next().apply(nextHolder.get()));
                }
            });
            return list.iterator();
        });
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }


}
