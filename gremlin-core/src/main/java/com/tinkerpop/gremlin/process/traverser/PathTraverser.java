package com.tinkerpop.gremlin.process.traverser;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.ImmutablePath;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends SimpleTraverser<T> {

    private Path path;

    protected PathTraverser() {
        super();
    }

    public PathTraverser(final T t, final Step<T, ?> step) {
        this.t = t;
        this.sideEffects = step.getTraversal().asAdmin().getSideEffects();
        this.sideEffects.getSackInitialValue().ifPresent(supplier -> this.sack = supplier.get());
        this.path = new ImmutablePath(step.getLabel(), t);
    }

    @Override
    public Traversal.SideEffects getSideEffects() {
        return this.sideEffects;
    }

    @Override
    public Path path() {
        return this.path;
    }

    @Override
    public <R> PathTraverser<R> split(final String label, final R r) {
        final PathTraverser<R> traverser = new PathTraverser<>();
        traverser.t = r;
        traverser.sideEffects = this.sideEffects;
        traverser.loops = this.loops;
        traverser.path = this.path.clone().extend(label, r);
        traverser.future = this.future;
        traverser.bulk = this.bulk;
        traverser.sack = null == this.sack ? null : this.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(this.sack);
        return traverser;
    }

    @Override
    public PathTraverser<T> split() {
        final PathTraverser<T> traverser = new PathTraverser<>();
        traverser.t = this.t;
        traverser.sideEffects = this.sideEffects;
        traverser.loops = this.loops;
        traverser.path = this.path.clone();
        traverser.future = this.future;
        traverser.bulk = this.bulk;
        traverser.sack = null == this.sack ? null : this.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(this.sack);
        return traverser;
    }

    @Override
    public PathTraverser<T> detach() {
        super.detach();
        this.path = DetachedFactory.detach(this.path.clone(), true);
        return this;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof PathTraverser)
                && ((PathTraverser) object).path().equals(this.path)
                && ((PathTraverser) object).get().equals(this.t)
                && ((PathTraverser) object).getFuture().equals(this.getFuture())
                && ((PathTraverser) object).loops() == this.loops()
                && (null == this.sack);
    }
}
