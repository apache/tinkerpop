package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import com.tinkerpop.gremlin.process.traverser.TraverserRequirement;
import org.javatuples.Pair;

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
public final class FoldStep<S, E> extends MapStep<S, E> implements Reducing<E, S> {

    private static final Set<TraverserRequirement> REQUIREMENTS = new HashSet<>(Arrays.asList(
            TraverserRequirement.BULK,
            TraverserRequirement.OBJECT
    ));

    private final Supplier<E> seed;
    private final BiFunction<E, S, E> foldFunction;

    public FoldStep(final Traversal traversal) {
        this(traversal, () -> (E) new ArrayList<S>(), (seed, start) -> {
            ((List) seed).add(start);
            return seed;
        });
    }

    public FoldStep(final Traversal traversal, final Supplier<E> seed, final BiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.seed = seed;
        this.foldFunction = foldFunction;
        FoldStep.generateFunction(this);
    }

    @Override
    public Pair<Supplier<E>, BiFunction<E, S, E>> getReducer() {
        return Pair.with(this.seed, this.foldFunction);
    }

    @Override
    public FoldStep<S, E> clone() throws CloneNotSupportedException {
        final FoldStep<S, E> clone = (FoldStep<S, E>) super.clone();
        FoldStep.generateFunction(clone);
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    /////////

    // TODO: should implement AbstractStep so it can set the bulk to 1
    public static <S, E> void generateFunction(final FoldStep<S, E> foldStep) {
        foldStep.setFunction(traverser -> {
            E mutatingSeed = foldStep.seed.get();
            for (long i = 0; i < traverser.bulk(); i++) {
                mutatingSeed = foldStep.foldFunction.apply(mutatingSeed, traverser.get());
            }
            while (foldStep.starts.hasNext()) {
                final Traverser<S> nextStart = foldStep.starts.next();
                for (long i = 0; i < nextStart.bulk(); i++) {
                    mutatingSeed = foldStep.foldFunction.apply(mutatingSeed, nextStart.get());
                }
            }
            return mutatingSeed;
        });
    }

}
