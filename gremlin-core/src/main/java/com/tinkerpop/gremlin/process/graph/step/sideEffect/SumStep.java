package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.SumMapReduce;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.process.util.AbstractStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SumStep extends AbstractStep<Number, Double> implements SideEffectCapable, MapReducer<MapReduce.NullObject, Double, MapReduce.NullObject, Double, Double> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT,
            TraverserRequirement.SIDE_EFFECTS
    ));

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

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
