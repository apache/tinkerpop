package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderIterator<T> implements Iterator<Traverser<T>> {

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

    public Traverser<T> next() {
        return this.trackPaths ?
                new PathTraverser<>(this.step.getAs(), this.iterator.next()) :
                new SimpleTraverser<>(this.iterator.next());
    }
}

