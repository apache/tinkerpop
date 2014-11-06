package com.tinkerpop.gremlin.process.traversers;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.ImmutablePath;
import com.tinkerpop.gremlin.process.util.PathAwareSideEffects;
import com.tinkerpop.gremlin.structure.util.referenced.ReferencedFactory;

import java.util.Collections;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends SimpleTraverser<T> {

    private Path path;

    protected PathTraverser() {
        super();
    }

    public PathTraverser(final T t, final Traversal.SideEffects sideEffects) {
        super(t, sideEffects);
        this.path = new ImmutablePath(Collections.emptySet(), t);
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
    public <R> PathTraverser<R> makeChild(final String label, final R r) {
        final PathTraverser<R> traverser = new PathTraverser<>();
        traverser.t = r;
        traverser.sideEffects = this.sideEffects;
        traverser.loops = this.loops;
        traverser.path = this.path.clone().extend(label, r);
        traverser.future = this.future;
        traverser.bulk = this.bulk;
        return traverser;
    }

    @Override
    public PathTraverser<T> makeSibling() {
        final PathTraverser<T> traverser = new PathTraverser<>();
        traverser.t = this.t;
        traverser.sideEffects = this.sideEffects;
        traverser.loops = this.loops;
        traverser.path = this.path.clone();
        traverser.future = this.future;
        traverser.bulk = this.bulk;
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
                && ((PathTraverser) object).get().equals(this.t)
                && ((PathTraverser) object).getFuture().equals(this.getFuture())
                && ((PathTraverser) object).loops() == this.loops()
                && ((PathTraverser) object).path().equals(this.path);
    }
}
