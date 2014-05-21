package com.tinkerpop.gremlin.process;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTraverser<T> extends SimpleTraverser<T> {

    private Path path = new Path();

    public PathTraverser(final T t) {
        super(t);
    }

    public PathTraverser(final String as, final T t) {
        super(t);
        this.path.add(as, t);
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(final Path path) {
        this.path = path;
    }

    public <R> PathTraverser<R> makeChild(final String as, final R r) {
        final PathTraverser<R> traverser = new PathTraverser<>(r);
        traverser.loops = this.loops;
        traverser.path.add(this.path);
        traverser.path.add(as, r);
        traverser.future = this.future;
        return traverser;
    }

    public PathTraverser<T> makeSibling() {
        final PathTraverser<T> traverser = new PathTraverser<>(this.t);
        traverser.loops = this.loops;
        traverser.path.add(this.path);
        traverser.future = this.future;
        return traverser;
    }

    public boolean equals(final Object object) {
        if (object instanceof PathTraverser)
            return this.t.equals(((PathTraverser) object).get()) && this.path.equals(((PathTraverser) object).getPath());
        else
            return false;
    }
}
