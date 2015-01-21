package com.tinkerpop.gremlin.process.graph.step.sideEffect;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.step.util.LazyBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import com.tinkerpop.gremlin.structure.Graph;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class CountStep<S> extends LazyBarrierStep<S, Long> implements Reducing<Long, Traverser<S>> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK
    ));

    public static final String COUNT_KEY = Graph.Hidden.hide("count");

    public CountStep(final Traversal traversal) {
        super(traversal);
        this.setSeedSupplier(() -> 0l);
        this.setFunction((seed, start) -> seed + start.bulk());
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public Reducer<Long, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBarrierFunction(), true);
    }
}
