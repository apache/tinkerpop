package com.tinkerpop.gremlin.process.traverser;


import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.EmptyPath;
import com.tinkerpop.gremlin.process.util.SparsePath;

import java.util.Map;
import java.util.WeakHashMap;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTraverser<T> extends AbstractTraverser<T> {

    protected SimpleTraverser() {
    }

    public SimpleTraverser(final T t, final Step<T, ?> step) {
        super(t, step);
        this.path = getOrCreateFromCache(this.sideEffects).extend(step.getLabel(), t);
    }

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.future.hashCode() + this.loops;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof SimpleTraverser
                && ((SimpleTraverser) object).get().equals(this.t)
                && ((SimpleTraverser) object).getFuture().equals(this.getFuture())
                && ((SimpleTraverser) object).loops() == this.loops()
                && (null == this.sack);
    }

    @Override
    public Traverser.Admin<T> detach() {
        final Path tempPath = this.path;
        this.path = EmptyPath.instance();
        super.detach();
        this.path = tempPath;
        return this;
    }

    //////////////////////

    private static final Map<TraversalSideEffects, Path> PATH_CACHE = new WeakHashMap<>();

    private static Path getOrCreateFromCache(final TraversalSideEffects sideEffects) {
        Path path = PATH_CACHE.get(sideEffects);
        if (null == path) {
            path = SparsePath.make();
            PATH_CACHE.put(sideEffects, path);
        }
        return path;
    }

}
