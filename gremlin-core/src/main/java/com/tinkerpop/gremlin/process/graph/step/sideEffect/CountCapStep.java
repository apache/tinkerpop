package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.CountCapMapReduce;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CountCapStep<S> extends SideEffectStep<S> implements SideEffectCapable, MapReducer<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    private static final String COUNT_KEY = Graph.Key.hide("count");

    public CountCapStep(final Traversal traversal) {
        super(traversal);
        this.setConsumer(traverser -> {
            final AtomicLong count = traverser.getSideEffects().getOrCreate(COUNT_KEY, () -> new AtomicLong(0l));
            count.set(count.get() + traverser.getBulk());
            traverser.getSideEffects().set(COUNT_KEY, count);
        });
    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().sideEffects().remove(COUNT_KEY);
    }

    @Override
    public String getSideEffectKey() {
        return COUNT_KEY;
    }

    @Override
    public MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> getMapReduce() {
        return new CountCapMapReduce(this);
    }
}
