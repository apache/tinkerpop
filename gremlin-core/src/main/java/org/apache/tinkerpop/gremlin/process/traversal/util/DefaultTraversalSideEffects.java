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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalSideEffects implements TraversalSideEffects {

    protected Map<String, Object> objectMap = new HashMap<>();
    protected Map<String, Supplier> supplierMap = new HashMap<>();
    protected UnaryOperator sackSplitOperator = null;
    protected Supplier sackInitialValue = null;

    public DefaultTraversalSideEffects() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerSupplier(final String key, final Supplier supplier) {
        this.supplierMap.put(key, supplier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return Optional.ofNullable(this.supplierMap.get(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        if (!this.supplierMap.containsKey(key))
            this.supplierMap.put(key, supplier);
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final Optional<UnaryOperator<S>> splitOperator) {
        this.sackInitialValue = initialValue;
        this.sackSplitOperator = splitOperator.orElse(null);
    }

    @Override
    public <S> Optional<Supplier<S>> getSackInitialValue() {
        return Optional.ofNullable(this.sackInitialValue);
    }

    @Override
    public <S> Optional<UnaryOperator<S>> getSackSplitOperator() {
        return Optional.ofNullable(this.sackSplitOperator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void set(final String key, final Object value) {
        SideEffectHelper.validateSideEffect(key, value);
        this.objectMap.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Optional<V> get(final String key) throws IllegalArgumentException {
        final V value = (V) this.objectMap.get(key);
        if (null != value)
            return Optional.of(value);
        else {
            if (this.supplierMap.containsKey(key)) {
                final V v = (V) this.supplierMap.get(key).get();
                this.objectMap.put(key, v);
                return Optional.of(v);
            } else {
                return Optional.empty();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> V getOrCreate(final String key, final Supplier<V> orCreate) {
        final Optional<V> optional = this.get(key);
        if (optional.isPresent())
            return optional.get();
        else {
            final V value = orCreate.get();
            this.objectMap.put(key, value);
            return value;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final String key) {
        this.objectMap.remove(key);
        this.supplierMap.remove(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> keys() {
        final Set<String> keys = new HashSet<>();
        keys.addAll(this.objectMap.keySet());
        keys.addAll(this.supplierMap.keySet());
        return Collections.unmodifiableSet(keys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLocalVertex(final Vertex vertex) {
        final Property<Map<String, Object>> property = vertex.property(SIDE_EFFECTS);
        if (property.isPresent()) {
            this.objectMap = property.value();
        } else {
            this.objectMap = new HashMap<>();
            vertex.property(VertexProperty.Cardinality.single, SIDE_EFFECTS, this.objectMap);
        }
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {
        this.objectMap.forEach(sideEffects::set);
        this.supplierMap.forEach(sideEffects::registerSupplierIfAbsent);
        // TODO: add sack information?
    }

    @Override
    public String toString() {
        return StringFactory.traversalSideEffectsString(this);
    }

    @Override
    @SuppressWarnings("CloneDoesntDeclareCloneNotSupportedException")
    public DefaultTraversalSideEffects clone() {
        try {
            final DefaultTraversalSideEffects sideEffects = (DefaultTraversalSideEffects) super.clone();
            sideEffects.objectMap = new HashMap<>(this.objectMap);
            sideEffects.supplierMap = new HashMap<>(this.supplierMap);
            return sideEffects;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
