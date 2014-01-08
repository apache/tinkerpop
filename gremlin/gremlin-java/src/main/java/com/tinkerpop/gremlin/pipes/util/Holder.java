package com.tinkerpop.gremlin.pipes.util;


import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> implements Serializable {

    public static final String NONE = "none";

    private T t;
    private int loops = 0;
    private Path path = new Path();
    private String future = NONE;

    public Holder(final String as, final T t) {
        this.t = t;
        this.path.add(as, t);
    }

    private Holder(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(final Path path) {
        this.path = path;
    }

    public boolean isDone() {
        return this.future.equals(NONE);
    }

    public String getFuture() {
        return this.future;
    }

    public void setFuture(final String as) {
        this.future = as;
    }

    public int getLoops() {
        return this.loops;
    }

    public void incrLoops() {
        this.loops++;
    }

    public <R> Holder<R> makeChild(final String as, final R r) {
        final Holder<R> holder = new Holder<>(r);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.add(as, r);
        holder.future = this.future;
        return holder;
    }

    public Holder<T> makeSibling() {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.future = this.future;
        return holder;
    }

    public Holder<T> makeSibling(final String as) {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.asNames.remove(holder.path.asNames.size() - 1);
        holder.path.asNames.add(as);
        holder.future = this.future;
        return holder;
    }

    public String toString() {
        return t.toString();
    }
}
