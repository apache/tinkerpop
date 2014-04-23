package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.SimpleHolder;
import com.tinkerpop.gremlin.process.Step;

import java.io.Serializable;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Iterator<T> iterator;
    private final Step step;
    private final boolean trackPaths;

    public HolderIterator(final Step step, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.step = step;
        this.trackPaths = true;
    }

    public HolderIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
        this.step = null;
        this.trackPaths = false;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        return this.trackPaths ?
                new PathHolder<>(this.step.getAs(), this.iterator.next()) :
                new SimpleHolder<>(this.iterator.next());
    }
}

