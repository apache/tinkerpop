package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.computer.MapReduce;
import com.tinkerpop.gremlin.process.graph.marker.MapReducer;
import com.tinkerpop.gremlin.process.graph.marker.SideEffectCapable;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.mapreduce.CountMapReduce;
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
public final class CountStep<S> extends AbstractStep<S, Long> implements SideEffectCapable, MapReducer<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.SIDE_EFFECTS
    ));

    public static final String COUNT_KEY = Graph.Hidden.hide("count");

    public CountStep(final Traversal traversal) {
        super(traversal);
    }

    @Override
    public Traverser<Long> processNextStart() {
        long counter = this.getTraversal().asAdmin().getSideEffects().getOrCreate(COUNT_KEY, () -> 0l);
        try {
            while (true) {
                counter = counter + this.starts.next().bulk();
            }
        } catch (final NoSuchElementException e) {
            this.getTraversal().asAdmin().getSideEffects().set(COUNT_KEY, counter);
        }
        throw FastNoSuchElementException.instance();
    }

    @Override
    public void reset() {
        super.reset();
        this.getTraversal().asAdmin().getSideEffects().remove(COUNT_KEY);
    }

    @Override
    public String getSideEffectKey() {
        return COUNT_KEY;
    }

    @Override
    public MapReduce<MapReduce.NullObject, Long, MapReduce.NullObject, Long, Long> getMapReduce() {
        return new CountMapReduce(this);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
