package com.tinkerpop.gremlin.pipes.util;


import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> implements Serializable {

    private T t;
    private int pipeIndex = 0;
    private int loops = 0;
    private Path path = new Path();

    public Holder(final String as, final T t) {
        this.t = t;
        this.path.add(as, t);
    }

    public Holder(final T t, final Path path) {
        this.t = t;
        this.path = path;
    }

    private Holder(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(T t) {
        this.t = t;
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(final Path path) {
        this.path = path;
    }

    public void setPipeIndex(final int index) {
        this.pipeIndex = index;
    }

    public int getPipeIndex() {
        return this.pipeIndex;
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

    public <R> Holder<R> makeChild(final String as, final R r) {
        final Holder<R> holder = new Holder<>(r);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.add(as, r);
        return holder;
    }

    public Holder<T> makeSibling() {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        return holder;
    }

    public Holder<T> makeSibling(final String as) {
        final Holder<T> holder = new Holder<>(this.t);
        holder.loops = this.loops;
        holder.path.add(this.path);
        holder.path.asNames.remove(holder.path.asNames.size() - 1);
        holder.path.asNames.add(as);
        return holder;
    }
}
