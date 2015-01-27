package com.tinkerpop.gremlin.process.computer.traversal;

import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class VertexTraversalSideEffects implements TraversalSideEffects {

    private static final UnsupportedOperationException EXCEPTION = new UnsupportedOperationException(VertexTraversalSideEffects.class.getSimpleName() + " is a read only facade to the underlying sideEffect at the local vertex");

    private Map<String, Object> objectMap;

    private VertexTraversalSideEffects() {

    }

    @Override
    public void registerSupplier(final String key, final Supplier supplier) {
        throw EXCEPTION;
    }

    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        throw EXCEPTION;
    }

    @Override
    public void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        throw EXCEPTION;
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final Optional<UnaryOperator<S>> splitOperator) {
        throw EXCEPTION;
    }

    @Override
    public <S> Optional<Supplier<S>> getSackInitialValue() {
        throw EXCEPTION;
    }

    @Override
    public <S> Optional<UnaryOperator<S>> getSackSplitOperator() {
        throw EXCEPTION;
    }

    @Override
    public boolean exists(final String key) {
        return this.objectMap.containsKey(key);
    }

    @Override
    public void set(final String key, final Object value) {
        throw EXCEPTION;
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        final V value = (V) this.objectMap.get(key);
        if (null != value)
            return value;
        else
            throw TraversalSideEffects.Exceptions.sideEffectDoesNotExist(key);
    }

    @Override
    public <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        throw EXCEPTION;
    }

    @Override
    public void remove(final String key) {
        throw EXCEPTION;
    }

    @Override
    public Set<String> keys() {
        return Collections.unmodifiableSet(this.objectMap.keySet());
    }

    @Override
    public void setLocalVertex(final Vertex vertex) {
        this.objectMap = vertex.<Map<String, Object>>property(SIDE_EFFECTS).orElse(new HashMap<>());
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {
        throw EXCEPTION;
    }

    @Override
    public String toString() {
        return StringFactory.traversalSideEffectsString(this);
    }

    @Override
    public VertexTraversalSideEffects clone() throws CloneNotSupportedException {
        return (VertexTraversalSideEffects) super.clone();
    }

    /////

    public static TraversalSideEffects of(final Vertex vertex) {
        final TraversalSideEffects sideEffects = new VertexTraversalSideEffects();
        sideEffects.setLocalVertex(vertex);
        return sideEffects;
    }
}
