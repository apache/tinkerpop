package com.tinkerpop.gremlin.pipes.util;


import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> implements Serializable {

    private T t;
    private int loops = 0;
    private Path path = new Path();

    public Holder(final String name, final T t) {
        this.t = t;
        this.path.add(name, t);
    }

    public Holder(final T t, final Path path) {
        this.t = t;
        this.path = path;
    }

    private Holder(final T t) {
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

    public <R> Holder<R> makeChild(final String name, final R r) {
        final Holder<R> holder = new Holder<>(r);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.add(name, r);
        return holder;
    }

    public Holder<T> makeSibling() {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        return holder;
    }

    public Holder<T> makeSibling(final String name) {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.names.remove(holder.path.names.size() - 1);
        holder.path.names.add(name);
        return holder;
    }
}
