package com.tinkerpop.gremlin.pipes.util;

import com.tinkerpop.gremlin.pipes.Pipeline;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Holder head;
    private final Iterator<T> iterator;
    private final Pipeline pipeline;

    public <P extends Pipeline> HolderIterator(final P pipeline, final Iterator<T> iterator) {
        this.pipeline = pipeline;
        this.iterator = iterator;
        this.head = null;
    }

    public <P extends Pipeline> HolderIterator(final P pipeline, final Holder head, final Iterator<T> iterator) {
        this.pipeline = pipeline;
        this.iterator = iterator;
        this.head = head;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        return null == this.head ? new Holder<>(this.pipeline, this.iterator.next()) : new Holder<>(this.pipeline, this.iterator.next(), this.head);
    }
}
