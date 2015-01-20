package com.tinkerpop.gremlin.process.util;

import com.tinkerpop.gremlin.process.TraversalSideEffects;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class EmptyTraversalSideEffects implements TraversalSideEffects {

    private static final EmptyTraversalSideEffects INSTANCE = new EmptyTraversalSideEffects();

    private EmptyTraversalSideEffects() {

    }

    @Override
    public void set(final String key, final Object value) {

    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectDoesNotExist(key);
    }

    @Override
    public void remove(final String key) {

    }

    @Override
    public Set<String> keys() {
        return Collections.emptySet();
    }

    @Override
    public void registerSupplier(final String key, final Supplier supplier) {

    }

    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return Optional.empty();
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final Optional<UnaryOperator<S>> splitOperator) {

    }

    @Override
    public <S> Optional<Supplier<S>> getSackInitialValue() {
        return Optional.empty();
    }

    @Override
    public <S> Optional<UnaryOperator<S>> getSackSplitOperator() {
        return Optional.empty();
    }

    @Override
    public void setLocalVertex(final Vertex vertex) {

    }

    @Override
    public TraversalSideEffects clone() throws CloneNotSupportedException {
        return this;
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {

    }

    public static EmptyTraversalSideEffects instance() {
        return INSTANCE;
    }
}
