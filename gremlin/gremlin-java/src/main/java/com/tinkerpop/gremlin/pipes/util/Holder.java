package com.tinkerpop.gremlin.pipes.util;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Holder<T> {

    private T t;
    private final List history = new ArrayList<>();

    public Holder(final T t) {
        this.t = t;
    }

    public Holder(final T t, Holder head) {
        this(t);
        Objects.requireNonNull(head);
        this.history.addAll(head.getHistory());
        this.history.add(head.get());
    }

    public <T> T get() {
        return (T) this.t;
    }

    public List getHistory() {
        return this.history;
    }


    public String toString() {
        return t.toString();
    }

}
