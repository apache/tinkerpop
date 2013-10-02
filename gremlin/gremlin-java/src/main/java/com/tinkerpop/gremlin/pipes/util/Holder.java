package com.tinkerpop.gremlin.pipes.util;


import com.tinkerpop.gremlin.pipes.Pipeline;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> {

    private T t;
    private int loops = 0;
    private final Path path = new Path();
    private final Pipeline pipeline;

    public <P extends Pipeline> Holder(final P pipeline, final T t) {
        this.pipeline = pipeline;
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
        final Holder<R> holder = new Holder<>(this.pipeline, r);
        holder.loops = this.loops;
        holder.path.addAll(this.path);
        holder.path.add(this.t);
        return holder;
    }

    public <R> Holder<R> makeSibling(final R r) {
        final Holder<R> holder = new Holder<>(this.pipeline, r);
        holder.loops = this.loops;
        holder.path.addAll(this.path);
        return holder;
    }

}
