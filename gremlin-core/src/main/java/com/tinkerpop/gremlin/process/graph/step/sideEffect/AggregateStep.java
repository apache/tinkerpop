package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.Bulkable;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.Reversible;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.marker.VertexCentric;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.AggregateMapReduce;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.function.SFunction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AggregateStep<S> extends AbstractStep<S, S> implements SideEffectCapable, Reversible, Bulkable, VertexCentric, MapReducer<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> {

    public final SFunction<S, ?> preAggregateFunction;
    Collection aggregate;
    final Queue<Traverser<S>> aggregateTraversers = new LinkedList<>();
    private long bulkCount = 1l;

    public AggregateStep(final Traversal traversal, final SFunction<S, ?> preAggregateFunction) {
        super(traversal);
        this.preAggregateFunction = preAggregateFunction;
        this.aggregate = this.traversal.memory().getOrCreate(this.getAs(), ArrayList::new);
    }

    public AggregateStep(final Traversal traversal) {
        this(traversal, null);
    }

    public void setCurrentBulkCount(final long bulkCount) {
        this.bulkCount = bulkCount;
    }

    @Override
    public void setAs(final String as) {
        this.traversal.memory().move(this.getAs(), as, ArrayList::new);
        super.setAs(as);
    }

    public void setCurrentVertex(final Vertex vertex) {
        final String hiddenAs = Graph.Key.hide(this.getAs());
        this.aggregate = vertex.<Collection>property(hiddenAs).orElse(new ArrayList());
        if (!vertex.property(hiddenAs).isPresent())
            vertex.property(hiddenAs, this.aggregate);
    }

    public MapReduce<MapReduce.NullObject, Object, MapReduce.NullObject, Object, List<Object>> getMapReduce() {
        return new AggregateMapReduce(this);
    }

    // TODO: Make work for vertex centric computations
    protected Traverser<S> processNextStart() {
        while (true) {
            if (this.starts.hasNext()) {
                this.starts.forEachRemaining(traverser -> {
                    for (int i = 0; i < this.bulkCount; i++) {
                        this.aggregate.add(null == this.preAggregateFunction ?
                                traverser.get() :
                                this.preAggregateFunction.apply(traverser.get()));
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
