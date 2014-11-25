package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.Path;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathAwareSideEffects implements Traversal.SideEffects {

    private final Path path;
    private final Traversal.SideEffects sideEffects;

    public PathAwareSideEffects(final Path path, final Traversal.SideEffects sideEffects) {
        this.path = path;
        this.sideEffects = sideEffects;
    }

    @Override
    public void set(final String key, final Object value) {
        this.sideEffects.set(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        if (this.path.hasLabel(key)) {
            return this.path.get(key);
        } else {
            return this.sideEffects.get(key);
        }
    }

    @Override
    public void registerSupplier(final String key, final Supplier supplier) {
        this.sideEffects.set(key, supplier);
    }

    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return this.sideEffects.getRegisteredSupplier(key);
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final Optional<UnaryOperator<S>> splitOperator) {
        this.sideEffects.setSack(initialValue, splitOperator);
    }

    @Override
    public <S> Optional<Supplier<S>> getSackInitialValue() {
        return this.sideEffects.getSackInitialValue();
    }

    @Override
    public <S> Optional<UnaryOperator<S>> getSackSplitOperator() {
        return this.sideEffects.getSackSplitOperator();
    }

    @Override
    public void remove(final String key) {
        this.sideEffects.remove(key);
    }

    @Override
    public Set<String> keys() {
        return this.sideEffects.keys();
    }

    @Override
    public void setLocalVertex(final Vertex vertex) {
        this.sideEffects.setLocalVertex(vertex);
    }
}
