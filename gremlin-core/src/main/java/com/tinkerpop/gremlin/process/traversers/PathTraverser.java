package com.tinkerpop.gremlin.process.traversers;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.ImmutablePath;
import com.tinkerpop.gremlin.process.util.PathAwareSideEffects;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends SimpleTraverser<T> {

    private Path path;

    protected PathTraverser() {
        super();
    }

    public PathTraverser(final String label, final T t, final Traversal.SideEffects sideEffects) {
        super(t, sideEffects);
        this.path = new ImmutablePath(label, t);
    }

    @Override
    public Traversal.SideEffects sideEffects() {
        if (null != this.sideEffects && !(this.sideEffects instanceof PathAwareSideEffects))
            this.sideEffects = new PathAwareSideEffects(this.path, this.sideEffects);
        return this.sideEffects;
    }

    @Override
    public boolean hasPath() {
        return true;
    }

    @Override
    public Path path() {
        return this.path;
    }

    @Override
    public void setPath(final Path path) {
        this.path = path;
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
        traverser.sack = this.sideEffects.getSackSplitOperator().isPresent() ?
                this.sideEffects.getSackSplitOperator().get().apply(this.sack) :
                this.sack;
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
        traverser.sack = this.sideEffects.getSackSplitOperator().isPresent() ?
                this.sideEffects.getSackSplitOperator().get().apply(this.sack) :
                this.sack;
        return traverser;
    }

    @Override
    public PathTraverser<T> detach() {
        super.detach();
        this.path = ReferencedFactory.detach(this.path.clone());
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
                && (null == this.sack) || this.sideEffects.getSackMergeOperator().isPresent();
    }
}
