package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.process.util.PathAwareSideEffects;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends SimpleTraverser<T> {

    private Path path = new Path();

    protected PathTraverser() {
        super();
    }

    public PathTraverser(final T t, final Traversal.SideEffects sideEffects) {
        super(t, sideEffects);
        this.sideEffects = new PathAwareSideEffects(this.path, this.sideEffects);
    }

    public PathTraverser(final String as, final T t, final Traversal.SideEffects sideEffects) {
        super(t, sideEffects);
        this.path.add(as, t);
        this.sideEffects = new PathAwareSideEffects(this.path, this.sideEffects);
    }

    @Override
    public boolean hasPath() {
        return true;
    }

    @Override
    public Path getPath() {
        return this.path;
    }

    @Override
    public void setPath(final Path path) {
        this.path = path;
    }

    @Override
    public <R> PathTraverser<R> makeChild(final String label, final R r) {
        final PathTraverser<R> traverser = new PathTraverser<>(r, this.sideEffects);
        traverser.loops = this.loops;
        traverser.path.add(this.path);
        traverser.path.add(label, r);
        traverser.future = this.future;
        return traverser;
    }

    @Override
    public PathTraverser<T> makeSibling() {
        final PathTraverser<T> traverser = new PathTraverser<>(this.t, this.sideEffects);
        traverser.loops = this.loops;
        traverser.path.add(this.path);
        traverser.future = this.future;
        return traverser;
    }

    @Override
    public PathTraverser<T> deflate() {
        super.deflate();
        this.path = ReferencedFactory.detach(this.path);
        return this;
    }

    @Override
    public int hashCode() {
        return super.hashCode() + this.path.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return (object instanceof PathTraverser)
                && ((PathTraverser) object).get().equals(this.t)
                && ((PathTraverser) object).getFuture().equals(this.getFuture())
                && ((PathTraverser) object).getLoops() == this.getLoops()
                && ((PathTraverser) object).getPath().equals(this.path);
    }
}
