package com.tinkerpop.gremlin.process.graph.traversal.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.traversal.step.Reducing;
import com.tinkerpop.gremlin.process.graph.traversal.step.util.ReducingBarrierStep;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends ReducingBarrierStep<S, E> implements Reducing<E, S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(TraverserRequirement.OBJECT));

    public FoldStep(final Traversal.Admin traversal) {
        this(traversal, () -> (E) new ArrayList<S>(), (seed, start) -> {
            ((List) seed).add(start);
            return seed;
        });
    }

    public FoldStep(final Traversal.Admin traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.setSeedSupplier(seed);
        this.setBiFunction(new ObjectBiFunction<>(foldFunction));
    }

    @Override
    public Reducer<E, S> getReducer() {
        return new Reducer<>(this.getSeedSupplier(), ((ObjectBiFunction<S, E>) this.getBiFunction()).getBiFunction(), false);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }
}
