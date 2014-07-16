package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.util.function.SBiFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FoldStep<S, E> extends MapStep<S, E> {

    private final AtomicReference<E> seed;
    public SBiFunction<E, S, E> foldFunction;

    public FoldStep(final Traversal traversal) {
        super(traversal);
        this.seed = null;
        this.setFunction(traverser -> {
            final List<S> list = new ArrayList<>();
            final S s = traverser.get();
            list.add(s);
            this.getPreviousStep().forEachRemaining(t -> list.add(t.get()));
            return (E) list;
        });
    }

    public FoldStep(final Traversal traversal, final E seed, final SBiFunction<E, S, E> foldFunction) {
        super(traversal);
        this.seed = new AtomicReference<>(seed);
        this.foldFunction = foldFunction;
        this.setFunction(traverser -> {
            this.seed.set(this.foldFunction.apply(this.seed.get(), traverser.get()));
            this.getPreviousStep().forEachRemaining(previousTraverser -> this.seed.set(this.foldFunction.apply(this.seed.get(), previousTraverser.get())));
            return (E) this.seed.get();
        });
    }
}
