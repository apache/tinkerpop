package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.FunctionRing;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateStep<S> extends AbstractStep<S, S> implements Reversible, Bulkable {

    public final FunctionRing<S, ?> functionRing;
    final Collection aggregate;
    final Queue<Traverser<S>> aggregateTraversers = new LinkedList<>();
    private long bulkCount = 1l;
    public String variable;

    public AggregateStep(final Traversal traversal, final String variable, final SFunction<S, ?>... preAggregateFunctions) {
        super(traversal);
        this.variable = variable;
        this.functionRing = new FunctionRing<>(preAggregateFunctions);
        this.aggregate = this.traversal.memory().getOrCreate(this.variable, ArrayList::new);
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    protected Traverser<S> processNextStart() {
        while (true) {
            if (this.starts.hasNext()) {
                this.starts.forEachRemaining(traverser -> {
                    for (int i = 0; i < this.bulkCount; i++) {
                        this.aggregate.add(this.functionRing.next().apply(traverser.get()));
                        this.aggregateTraversers.add(traverser.makeSibling());
                    }
                });
            } else {
                if (!this.aggregateTraversers.isEmpty())
                    return this.aggregateTraversers.remove().makeSibling();
                else
                    throw FastNoSuchElementException.instance();
            }
        }
    }
}
