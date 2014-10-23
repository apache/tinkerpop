package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
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
    public void set(T t) {

    }

    @Override
    public void setPath(Path path) {

    }

    @Override
    public void incrLoops() {

    }

    @Override
    public void resetLoops() {

    }

    @Override
    public String getFuture() {
        return HALT;
    }

    @Override
    public void setFuture(String label) {

    }

    @Override
    public void setBulk(long count) {

    }

    @Override
    public <R> Admin<R> makeChild(final String label, final R r) {
        return INSTANCE;
    }

    @Override
    public Admin<T> makeSibling() {
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
    public void setSideEffects(final Traversal.SideEffects sideEffects) {

    }

    @Override
    public T get() {
        return null;
    }

    @Override
    public Path path() {
        return EmptyPath.instance();
    }

    @Override
    public boolean hasPath() {
        return true;
    }

    @Override
    public short loops() {
        return 0;
    }

    @Override
    public long bulk() {
        return 0l;
    }

    @Override
    public Traversal.SideEffects sideEffects() {
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
}
