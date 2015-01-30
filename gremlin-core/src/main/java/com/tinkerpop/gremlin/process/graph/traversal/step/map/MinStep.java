package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MinStep<S extends Number> extends ReducingBarrierStep<S, S> implements Reducing<S, Traverser<S>> {

    public MinStep(final Traversal traversal) {
        super(traversal);
        this.setSeedSupplier(() -> (S) Double.valueOf(Double.MAX_VALUE));
        this.setBiFunction((seed, start) -> seed.doubleValue() < start.get().doubleValue() ? seed : start.get());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public Reducer<S, Traverser<S>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true);
    }
}