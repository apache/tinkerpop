package com.tinkerpop.gremlin.process.traverser.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedElement;
import com.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractPathTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    protected T t;
    protected Object sack = null;
    protected String future = HALT;
    protected short loops = 0;  // an optimization hack to use a short internally to save bits :)
    protected transient TraversalSideEffects sideEffects;
    protected long bulk = 1l;
    protected Path path;

    protected AbstractPathTraverser() {

    }

    public AbstractPathTraverser(final T t, final Step<T, ?> step) {
        this.t = t;
        this.sideEffects = step.getTraversal().asAdmin().getSideEffects();
        this.sideEffects.getSackInitialValue().ifPresent(supplier -> this.sack = supplier.get());
    }

    /////////////////

    @Override
    public T get() {
        return this.t;
    }

    @Override
    public void set(final T t) {
        this.t = t;
    }

    /////////////////

    @Override
    public Path path() {
        return this.path;
    }

    /////////////////

    @Override
    public <S> S sack() {
        return (S) this.sack;
    }

    @Override
    public <S> void sack(final S object) {
        this.sack = object;
    }

    /////////////////

    @Override
    public void setBulk(final long count) {
        this.bulk = count;
    }

    @Override
    public long bulk() {
        return this.bulk;
    }

    /////////////////

    @Override
    public int loops() {
        return this.loops;
    }

    @Override
    public void incrLoops(final String stepLabel) {
        this.loops++;
    }

    @Override
    public void resetLoops() {
        this.loops = 0;
    }

    /////////////////

    @Override
    public String getFutureId() {
        return this.future;
    }

    @Override
    public void setFutureId(final String stepId) {
        this.future = stepId;
    }

    /////////////////

    @Override
    public TraversalSideEffects getSideEffects() {
        return this.sideEffects;
    }


    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
    }

    /////////////////

    @Override
    public Traverser.Admin<T> detach() {
        this.t = DetachedFactory.detach(this.t, false);
        this.path = DetachedFactory.detach(this.path, true);
        return this;
    }

    @Override
    public Traverser.Admin<T> attach(final Vertex vertex) {
        if (this.t instanceof DetachedElement)
            this.t = (T) ((DetachedElement) this.t).attach(vertex);
        else if (this.t instanceof DetachedProperty)
            this.t = (T) ((DetachedProperty) this.t).attach(vertex);
        // you do not want to attach a path because it will reference graph objects not at the current vertex
        return this;
    }

    /////////////////

    @Override
    public void merge(final Traverser.Admin<?> other) {
        this.bulk = this.bulk + other.bulk();
    }

    @Override
    public <R> Traverser.Admin<R> split(final R r, final Step<T, R> step) {
        try {
            final AbstractPathTraverser<R> clone = (AbstractPathTraverser<R>) super.clone();
            clone.t = r;
            final Optional<String> stepLabel = step.getLabel();
            clone.path = stepLabel.isPresent() ? clone.path.clone().extend(r, stepLabel.get()) : clone.path.clone().extend(r);
            clone.sack = null == clone.sack ? null : clone.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(clone.sack);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Traverser.Admin<T> split() {
        try {
            final AbstractPathTraverser<T> clone = (AbstractPathTraverser<T>) super.clone();
            clone.sack = null == clone.sack ? null : clone.sideEffects.getSackSplitOperator().orElse(UnaryOperator.identity()).apply(clone.sack);
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /////////////////

    @Override
    public AbstractPathTraverser<T> clone() throws CloneNotSupportedException {
        return (AbstractPathTraverser<T>) super.clone();
    }

    @Override
    public int hashCode() {
        return this.t.hashCode() + this.future.hashCode() + this.loops;
    }

    @Override
    public String toString() {
        return this.t.toString();
    }
}
