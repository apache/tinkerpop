package com.tinkerpop.gremlin.pipes.util;


import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> {

    private T t;
    private int loops = 0;
    private final Path path = new Path();

    public <P extends Pipeline> Holder(final T t) {
        this.t = t;
    }

    public <T> T get() {
        return (T) this.t;
    }

    public Path getPath() {
        return this.path;
    }

    public String toString() {
        return t.toString();
    }

    public int getLoops() {
        return this.loops;
    }

    public void incrLoops() {
        this.loops++;
    }

    public <R> Holder<R> makeChild(final R r) {
        final Holder<R> holder = new Holder<>(r);
        holder.loops = this.loops;
        holder.path.addAll(this.path);
        holder.path.add(this.t);
        return holder;
    }

    public Holder<T> makeSibling() {
        final Holder<T> holder = new Holder<>(t);
        holder.loops = this.loops;
        holder.path.addAll(this.path);
        return holder;
    }

}
