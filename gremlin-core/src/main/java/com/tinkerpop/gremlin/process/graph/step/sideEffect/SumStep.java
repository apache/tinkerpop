package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.SumMapReduce;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SumStep extends AbstractStep<Number, Double> implements SideEffectCapable, MapReducer<MapReduce.NullObject, Double, MapReduce.NullObject, Double, Double> {

    public static final String SUM_KEY = Graph.Hidden.hide("sum");

    public SumStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    public Traverser<Double> processNextStart() {
        double sum = this.getTraversal().asAdmin().getSideEffects().getOrCreate(SUM_KEY, () -> 0.0d);
        try {
            while (true) {
                final Traverser<Number> start = this.starts.next();
                sum = sum + (start.get().doubleValue() * start.bulk());
            }
        } catch (final NoSuchElementException e) {
            this.getTraversal().asAdmin().getSideEffects().set(SUM_KEY, sum);
        }
        throw FastNoSuchElementException.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().asAdmin().getSideEffects().remove(SUM_KEY);
    }

    @Override
    public String getSideEffectKey() {
        return SUM_KEY;
    }

    @Override
    public MapReduce<MapReduce.NullObject, Double, MapReduce.NullObject, Double, Double> getMapReduce() {
        return new SumMapReduce(this);
    }
}
