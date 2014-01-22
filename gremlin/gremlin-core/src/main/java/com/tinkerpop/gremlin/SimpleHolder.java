package com.tinkerpop.gremlin;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleHolder<T> implements Holder<T> {

    private static final String PATH_ERROR_MESSAGE = "Path tracking is not supported by this Holder: " + SimpleHolder.class;

    protected T t;
    protected String future = NO_FUTURE;
    protected int loops = 0;

    public SimpleHolder(final T t) {
        this.t = t;
    }

    public T get() {
        return this.t;
    }

    public void set(final T t) {
        this.t = t;
    }

    public String getFuture() {
        return this.future;
    }

    public void setFuture(final String as) {
        this.future = as;
    }

    public Path getPath() {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    public void setPath(final Path path) {
        throw new IllegalStateException(PATH_ERROR_MESSAGE);
    }

    public int getLoops() {
        return this.loops;
    }

    public void incrLoops() {
        this.loops++;
    }

    public <R> SimpleHolder<R> makeChild(final String as, final R r) {
        final SimpleHolder<R> holder = new SimpleHolder<>(r);
        holder.future = this.future;
        holder.loops = this.loops;
        return holder;
    }

    public SimpleHolder<T> makeSibling() {
        final SimpleHolder<T> holder = new SimpleHolder<>(this.t);
        holder.future = this.future;
        holder.loops = this.loops;
        return holder;
    }

    public String toString() {
        return t.toString();
    }

    public int hashCode() {
        return this.t.hashCode();
    }

    public boolean equals(final Object object) {
        if (object instanceof SimpleHolder)
            return this.t.equals(((SimpleHolder) object).get());
        else
            return false;
    }
}
