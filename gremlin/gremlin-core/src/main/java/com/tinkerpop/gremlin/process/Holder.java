package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Holder<T> extends Serializable {

    public static final String NO_FUTURE = "noFuture";

    public T get();

    public void set(final T t);

    public default boolean isDone() {
        return this.getFuture().equals(NO_FUTURE);
    }

    public Path getPath();

    public void setPath(final Path path);

    public int getLoops();

    public void incrLoops();

    public void resetLoops();

    public String getFuture();

    public void setFuture(final String as);

    public <R> Holder<R> makeChild(final String as, final R r);

    public Holder<T> makeSibling();

    public Holder<T> deflate();

    public Holder<T> inflate(final Vertex hostVertex);
}
