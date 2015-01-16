package com.tinkerpop.gremlin.process.traverser.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.util.EmptyPath;
import com.tinkerpop.gremlin.process.util.EmptyTraversalSideEffects;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    protected T t;

    protected AbstractTraverser() {

    }

    public AbstractTraverser(final T t) {
        this.t = t;
    }

    /////////////

    @Override
    public void merge(final Admin<?> other) {
        throw new UnsupportedOperationException("This traverser does not support merging: " + this.getClass().getCanonicalName());
    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        try {
            final AbstractTraverser<R> clone = (AbstractTraverser<R>) super.clone();
            clone.t = r;
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Admin<T> split() {
        try {
            return (AbstractTraverser<T>) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void set(final T t) {
        this.t = t;
    }

    @Override
    public void incrLoops(final String stepLabel) {

    }

    @Override
    public void resetLoops() {

    }

    @Override
    public String getFutureId() {
        throw new UnsupportedOperationException("This traverser does not support futures: " + this.getClass().getCanonicalName());
    }

    @Override
    public void setFutureId(final String stepId) {

    }

    @Override
    public void setBulk(final long count) {
        throw new UnsupportedOperationException("This traverser does not support bulking: " + this.getClass().getCanonicalName());
    }

    @Override
    public Admin<T> detach() {
        this.t = DetachedFactory.detach(this.t, false);
        return this;
    }

    @Override
    public Admin<T> attach(final Vertex hostVertex) {
        if (this.t instanceof DetachedElement)
            this.t = (T) ((DetachedElement) this.t).attach(hostVertex);
        else if (this.t instanceof DetachedProperty)
            this.t = (T) ((DetachedProperty) this.t).attach(hostVertex);
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        return this;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return EmptyTraversalSideEffects.instance();
        //throw new UnsupportedOperationException("This traverser does not support sideEffects: " + this.getClass().getCanonicalName());
    }

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public <S> S sack() {
        throw new UnsupportedOperationException("This traverser does not support sacks: " + this.getClass().getCanonicalName());
    }

    @Override
    public <S> void sack(final S object) {

    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public int loops() {
        throw new UnsupportedOperationException("This traverser does not support loops: " + this.getClass().getCanonicalName());
    }

    @Override
    public long bulk() {
        return 1;
    }

    @Override
    public AbstractTraverser<T> clone() throws CloneNotSupportedException {
        return (AbstractTraverser<T>) super.clone();
    }

    ///////////

    @Override
    public int hashCode() {
        return this.t.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof AbstractTraverser && ((AbstractTraverser) object).get().equals(this.t);
    }

    @Override
    public String toString() {
        return this.t.toString();
    }
}
