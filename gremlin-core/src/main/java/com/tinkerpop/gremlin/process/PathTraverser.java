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
        final PathTraverser<R> holder = new PathTraverser<>(r);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.add(as, r);
        holder.future = this.future;
        return holder;
    }

    public PathTraverser<T> makeSibling() {
        final PathTraverser<T> holder = new PathTraverser<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.future = this.future;
        return holder;
    }

    public boolean equals(final Object object) {
        if (object instanceof PathTraverser)
            return this.t.equals(((PathTraverser) object).get()) && this.path.equals(((PathTraverser) object).getPath());
        else
            return false;
    }
}
