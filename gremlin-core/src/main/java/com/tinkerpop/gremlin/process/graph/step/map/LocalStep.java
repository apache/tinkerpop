package com.tinkerpop.gremlin.process.graph.step.map;

import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.marker.PathConsumer;
import com.tinkerpop.gremlin.process.util.TraversalHelper;

import java.util.function.Supplier;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class LocalStep<S, E> extends FlatMapStep<S, E> implements PathConsumer {

    private final Supplier<Traversal<S, E>> localTraversalSupplier;
    private Traversal<S, E> localTraversal;

    public LocalStep(final Traversal traversal, final Supplier<Traversal<S, E>> localTraversalSupplier) {
        super(traversal);
        this.localTraversalSupplier = localTraversalSupplier;
        this.localTraversal = localTraversalSupplier.get();
        LocalStep.generateFunction(this);
    }

    public Supplier<Traversal<S, E>> getLocalTraversal() {
        return this.localTraversalSupplier;
    }

    @Override
    public LocalStep<S, E> clone() throws CloneNotSupportedException {
        final LocalStep<S, E> clone = (LocalStep<S, E>) super.clone();
        clone.localTraversal = this.localTraversal.clone();
        LocalStep.generateFunction(clone);
        return clone;
    }

    @Override
    public String toString() {
        return TraversalHelper.makeStepString(this, this.localTraversal);
    }

    @Override
    public boolean requiresPaths() {
        return TraversalHelper.trackPaths(this.localTraversal);
    }

    ////////////////

    private static final <S, E> void generateFunction(final LocalStep<S, E> localStep) {
        localStep.setFunction(traverser -> {
            localStep.localTraversal.reset();
            TraversalHelper.getStart(localStep.localTraversal).addStart(traverser);
            return localStep.localTraversal;
        });
    }
}
