package com.tinkerpop.gremlin;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathHolder<T> extends SimpleHolder<T> {

    private int loops = 0;
    private Path path = new Path();

    public PathHolder(final T t) {
        super(t);
    }

    public PathHolder(final String as, final T t) {
        super(t);
        this.path.add(as, t);
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(final Path path) {
        this.path = path;
    }

    public int getLoops() {
        return this.loops;
    }

    public void incrLoops() {
        this.loops++;
    }

    public <R> PathHolder<R> makeChild(final String as, final R r) {
        final PathHolder<R> holder = new PathHolder<>(r);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.add(as, r);
        holder.future = this.future;
        return holder;
    }

    public PathHolder<T> makeSibling() {
        final PathHolder<T> holder = new PathHolder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.future = this.future;
        return holder;
    }

    public SimpleHolder<T> makeSibling(final String as) {
        final PathHolder<T> holder = new PathHolder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.asNames.remove(holder.path.asNames.size() - 1);
        holder.path.asNames.add(as);
        holder.future = this.future;
        return holder;
    }
}
