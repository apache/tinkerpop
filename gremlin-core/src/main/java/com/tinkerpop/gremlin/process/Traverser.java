package com.tinkerpop.gremlin.process;

import com.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traverser<T> extends Serializable {

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

    public <R> Traverser<R> makeChild(final String as, final R r);

    public Traverser<T> makeSibling();

    public Traverser<T> deflate();

    public Traverser<T> inflate(final Vertex hostVertex);
}
