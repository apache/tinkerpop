package com.tinkerpop.gremlin.pipes.util;


import com.tinkerpop.gremlin.pipes.Pipeline;

import java.util.Objects;

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

    public <P extends Pipeline> Holder(final P pipeline, final T t, Holder head) {
        this(pipeline, t);
        Objects.requireNonNull(head);
        this.loops = head.getLoops();
        this.path.addAll(head.getPath());
        this.path.add(head.get());
    }

    public <T> T get() {
        return (T) this.t;
    }

    public Path getPath() {
        return this.path;
    }

    public <T> T getPath(final String key) {
        return (T) this.path.get(this.pipeline.getAs(key));
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

}
