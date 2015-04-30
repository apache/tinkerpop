/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
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
    public void set(final String key, final Object value) {
        throw EXCEPTION;
    }

    @Override
    public <V> Optional<V> get(final String key) throws IllegalArgumentException {
        return Optional.ofNullable((V) this.objectMap.get(key));
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
        this.objectMap = vertex.<Map<String, Object>>property(SIDE_EFFECTS).orElse(Collections.emptyMap());
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
    public VertexTraversalSideEffects clone() {
        try {
            return (VertexTraversalSideEffects) super.clone();
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /////

    public static TraversalSideEffects of(final Vertex vertex) {
        final TraversalSideEffects sideEffects = new VertexTraversalSideEffects();
        sideEffects.setLocalVertex(vertex);
        return sideEffects;
    }
}
