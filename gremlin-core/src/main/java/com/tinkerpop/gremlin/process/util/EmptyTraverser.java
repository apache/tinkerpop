package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraverser<T> implements Traverser<T>, Traverser.Admin<T> {

    private static final EmptyTraverser INSTANCE = new EmptyTraverser();

    public static <R> EmptyTraverser<R> instance() {
        return INSTANCE;
    }

    private EmptyTraverser() {

    }

    @Override
    public void set(final T t) {

    }

    @Override
    public void incrLoops(final String stepLabel) {

    }

    @Override
    public void resetLoops() {

    }

    @Override
    public String getFutureId() {
        return HALT;
    }

    @Override
    public void setFutureId(final String stepId) {

    }

    @Override
    public void setBulk(long count) {

    }

    @Override
    public <R> Admin<R> split(final R r, final Step<T, R> step) {
        return INSTANCE;
    }

    @Override
    public Admin<T> split() {
        return this;
    }

    @Override
    public Admin<T> detach() {
        return this;
    }

    @Override
    public Admin<T> attach(final Vertex hostVertex) {
        return this;
    }

    @Override
    public void setSideEffects(final TraversalSideEffects sideEffects) {

    }

    @Override
    public T get() {
        return null;
    }

    @Override
    public <S> S sack() {
        return null;
    }

    @Override
    public <S> void sack(final S object) {

    }

    @Override
    public void merge(final Traverser.Admin<?> other) {

    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public int loops() {
        return 0;
    }

    @Override
    public long bulk() {
        return 0l;
    }

    @Override
    public TraversalSideEffects getSideEffects() {
        return null;
    }

    @Override
    public int hashCode() {
        return 380473707;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyTraverser;
    }

    @Override
    public EmptyTraverser<T> clone() throws CloneNotSupportedException {
        return this;
    }
}
