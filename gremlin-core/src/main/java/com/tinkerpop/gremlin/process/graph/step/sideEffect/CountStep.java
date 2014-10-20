package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.CountMapReduce;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CountStep<S> extends AbstractStep<S, Long> implements SideEffectCapable, MapReducer<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    public static final String COUNT_KEY = Graph.System.system("count");

    public CountStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    public Traverser<Long> processNextStart() {
        long counter = this.getTraversal().sideEffects().getOrCreate(COUNT_KEY, () -> 0l);
        try {
            while (true) {
                counter = counter + this.starts.next().bulk();
            }
        } catch (final NoSuchElementException e) {
            this.getTraversal().sideEffects().set(COUNT_KEY, counter);
        }
        throw FastNoSuchElementException.instance();
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
        return new CountMapReduce();
    }
}
