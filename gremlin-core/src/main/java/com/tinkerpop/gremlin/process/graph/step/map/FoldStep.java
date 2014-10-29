package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.marker.Reducing;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends MapStep<S, E> implements Reducing<E, Traverser<S>> {

    private final Supplier<E> seed;
    private final BiFunction<E, Traverser<S>, E> foldFunction;

    public FoldStep(final Traversal traversal) {
        this(traversal, () -> (E) new ArrayList<S>(), (seed, traverser) -> {
            ((List) seed).add(traverser.get());
            return seed;
        });
    }

    public FoldStep(final Traversal traversal, final Supplier<E> seed, final BiFunction<E, Traverser<S>, E> foldFunction) {
        super(traversal);
        this.seed = seed;
        this.foldFunction = foldFunction;
        this.setFunction(traverser -> {
            E mutatingSeed = this.foldFunction.apply(this.seed.get(), traverser);
            while (this.starts.hasNext()) {
                mutatingSeed = this.foldFunction.apply(mutatingSeed, this.starts.next());
            }
            return mutatingSeed;
        });
    }

    @Override
    public Pair<Supplier<E>, BiFunction<E, Traverser<S>, E>> getReducer() {
        return Pair.with(this.seed, this.foldFunction);
    }

}
