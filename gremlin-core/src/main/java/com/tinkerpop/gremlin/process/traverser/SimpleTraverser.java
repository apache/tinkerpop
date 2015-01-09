package com.tinkerpop.gremlin.process.traverser;


import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.SparsePath;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SimpleTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    protected T t;
    protected Object sack = null;
    protected String future = HALT;
    protected short loops = 0;
    protected transient Traversal.SideEffects sideEffects;
    protected long bulk = 1l;
    protected Path path;

    protected SimpleTraverser() {
    }

    public SimpleTraverser(final T t, final Step<T, ?> step) {
        this.t = t;
        this.sideEffects = step.getTraversal().asAdmin().getSideEffects();
        this.sideEffects.getSackInitialValue().ifPresent(supplier -> this.sack = supplier.get());
        this.path = getOrCreateFromCache(this.sideEffects).extend(step.getLabel(), t);
    }

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public void set(final T t) {
        this.t = t;
    }

    ////////

    @Override
    public Path path() {
        return this.path;
    }

    ////////

    @Override
    public <S> S sack() {
        return (S) this.sack;
    }

    @Override
    public <S> void sack(final S object) {
        this.sack = object;
    }

    ////////

    public void setBulk(final long count) {
        this.bulk = count;
    }

    public long bulk() {
        return this.bulk;
    }

    @Override
    public Traversal.SideEffects getSideEffects() {
        return this.sideEffects;
    }

    ////////

    @Override
    public int loops() {
        return this.loops;
    }

    @Override
    public void incrLoops() {
        this.loops++;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    ////////

    @Override
    public String getFuture() {
        return this.future;
    }

    @Override
    public void setFuture(final String label) {
        this.future = label;
    }

    ////////
    @Override
    public void merge(final Traverser.Admin<?> other) {
        this.bulk = this.bulk + other.bulk();
    }

    @Override
    public <R> SimpleTraverser<R> split(final String label, final R r) {
        final SimpleTraverser<R> traverser = new SimpleTraverser<>();
        traverser.t = r;
        traverser.sideEffects = this.sideEffects;
        traverser.future = this.future;
        traverser.loops = this.loops;
        traverser.bulk = this.bulk;
        traverser.sack = null == this.sack ? null : this.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(this.sack);
        traverser.path = this.path.extend(label, r);
        return traverser;
    }

    @Override
    public SimpleTraverser<T> split() {
        final SimpleTraverser<T> traverser = new SimpleTraverser<>();
        traverser.t = t;
        traverser.sideEffects = this.sideEffects;
        traverser.future = this.future;
        traverser.loops = this.loops;
        traverser.bulk = this.bulk;
        traverser.sack = null == this.sack ? null : this.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(this.sack);
        traverser.path = this.path;
        return traverser;
    }

    @Override
    public void setSideEffects(final Traversal.SideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    @Override
    public String toString() {
        return this.t.toString();
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
    public SimpleTraverser<T> detach() {
        if (this.t instanceof Element) {
            this.t = (T) DetachedFactory.detach((Element) this.t, false);
        } else if (this.t instanceof Property) {
            this.t = (T) DetachedFactory.detach((Property) this.t);
        } else if (this.t instanceof Path) {
            this.t = (T) DetachedFactory.detach((Path) this.t, false);
        }
        return this;
    }

    @Override
    public SimpleTraverser<T> attach(final Vertex vertex) {
        if (this.t instanceof DetachedElement) {
            this.t = (T) ((DetachedElement) this.t).attach(vertex);
        } else if (this.t instanceof DetachedProperty) {
            this.t = (T) ((DetachedProperty) this.t).attach(vertex);
        }
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        return this;
    }

    //////////////////////

    private static final Map<Traversal.SideEffects, Path> PATH_CACHE = new WeakHashMap<>();

    private static Path getOrCreateFromCache(final Traversal.SideEffects sideEffects) {
        Path path = PATH_CACHE.get(sideEffects);
        if (null == path) {
            path = SparsePath.make();
            PATH_CACHE.put(sideEffects, path);
        }
        return path;
    }

}
