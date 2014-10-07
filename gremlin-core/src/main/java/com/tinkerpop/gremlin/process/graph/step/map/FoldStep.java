package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FoldStep<S, E> extends MapStep<S, E> {

    private final AtomicReference<E> mutatingSeed;
    private final E seed;
    private final BiFunction<E, Traverser<S>, E> foldFunction;

    public FoldStep(final Traversal traversal) {
        super(traversal);
        this.seed = null;
        this.mutatingSeed = null;
        this.foldFunction = null;
        this.setFunction(traverser -> {
            final List<S> list = new ArrayList<>();
            final S s = traverser.get();
            list.add(s);
            this.starts.forEachRemaining(t -> list.add(t.get()));
            return (E) list;
        });
    }

    public FoldStep(final Traversal traversal, final E seed, final BiFunction<E, Traverser<S>, E> foldFunction) {
        super(traversal);
        this.seed = seed;
        this.mutatingSeed = new AtomicReference<>(seed);
        this.foldFunction = foldFunction;
        this.setFunction(traverser -> {
            this.mutatingSeed.set(this.foldFunction.apply(this.mutatingSeed.get(), traverser));
            this.starts.forEachRemaining(previousTraverser -> this.mutatingSeed.set(this.foldFunction.apply(this.mutatingSeed.get(), previousTraverser)));
            return (E) this.mutatingSeed.get();
        });
    }

    @Override
    public void reset() {
        super.reset();
        if (null != this.seed) {
            this.mutatingSeed.set(this.seed);
        }
    }
}
