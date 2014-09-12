package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.PathTraverser;
import com.tinkerpop.gremlin.process.SimpleTraverser;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traverser;

import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraverserIterator<T> implements Iterator<Traverser<T>> {

    private final Iterator<T> iterator;
    private final Step step;
    private final boolean trackPaths;

    public TraverserIterator(final Step step, final boolean trackPaths, final Iterator<T> iterator) {
        this.iterator = iterator;
        this.step = step;
        this.trackPaths = trackPaths;
    }

    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public Traverser<T> next() {
        return this.trackPaths ?
                new PathTraverser<>(this.step.getLabel(), this.iterator.next(),this.step.getTraversal().sideEffects()) :
                new SimpleTraverser<>(this.iterator.next(),this.step.getTraversal().sideEffects());
    }
}

