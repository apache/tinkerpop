package com.tinkerpop.gremlin.process.oltp.util;

import com.tinkerpop.gremlin.process.Holder;
import com.tinkerpop.gremlin.process.PathHolder;
import com.tinkerpop.gremlin.process.Pipe;
import com.tinkerpop.gremlin.process.SimpleHolder;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Holder<T>> {

    private final Iterator<T> iterator;
    private final Pipe pipe;
    private final boolean trackPaths;

    public HolderIterator(final Pipe pipe, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.pipe = pipe;
        this.trackPaths = true;
    }

    public HolderIterator(final Iterator<T> iterator) {
        this.iterator = iterator;
        this.pipe = null;
        this.trackPaths = false;
    }

    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    public Holder<T> next() {
        return this.trackPaths ?
                new PathHolder<>(this.pipe.getAs(), this.iterator.next()) :
                new SimpleHolder<>(this.iterator.next());
    }
}
