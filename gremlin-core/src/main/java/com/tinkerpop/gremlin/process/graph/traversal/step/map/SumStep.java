package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traversal.step.Reducing;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SumStep extends ReducingBarrierStep<Number, Double> implements Reducing<Double, Traverser<Number>> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    );

    public SumStep(final Traversal.Admin traversal) {
        super(traversal);
        this.setSeedSupplier(() -> 0.0d);
        this.setBiFunction((seed, start) -> seed + (start.get().doubleValue() * start.bulk()));
    }


    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public Reducer<Double, Traverser<Number>> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), this.getBiFunction(), true);
    }
}