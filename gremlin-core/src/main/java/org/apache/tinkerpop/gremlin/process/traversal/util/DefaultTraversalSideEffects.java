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

import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalSideEffects implements TraversalSideEffects {

    protected Map<String, Object> objectMap = new HashMap<>();
    protected Map<String, Supplier> supplierMap = new HashMap<>();
    protected Map<String, BinaryOperator> reducerMap = new HashMap<>();
    protected UnaryOperator sackSplitOperator = null;
    protected BinaryOperator sackMergeOperator = null;
    protected Supplier sackInitialValue = null;

    public DefaultTraversalSideEffects() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> void register(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {
        SideEffectHelper.validateSideEffect(key, initialValue);
        this.supplierMap.put(key, initialValue);
        this.reducerMap.put(key, reducer);
    }

    @Override
    public <V> void registerIfAbsent(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {
        if (null == this.supplierMap.get(key)) {
            this.supplierMap.put(key, initialValue);
        }
        if (null == this.reducerMap.get(key)) {
            this.reducerMap.put(key, reducer);
        }
    }

    @Override
    public <V> BinaryOperator<V> getReducer(final String key) {
        return this.reducerMap.get(key);
    }

    @Override
    public <V> Supplier<V> getSupplier(final String key) {
        return this.supplierMap.get(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <S> void setSack(final Supplier<S> initialValue, final UnaryOperator<S> splitOperator, final BinaryOperator<S> mergeOperator) {
        this.sackInitialValue = initialValue;
        this.sackSplitOperator = splitOperator;
        this.sackMergeOperator = mergeOperator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <S> Supplier<S> getSackInitialValue() {
        return this.sackInitialValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <S> UnaryOperator<S> getSackSplitter() {
        return this.sackSplitOperator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <S> BinaryOperator<S> getSackMerger() {
        return this.sackMergeOperator;
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
    public void add(final String key, final Object value) {
        this.set(key, this.reducerMap.getOrDefault(key, Operator.assign).apply(this.get(key).get(), value));
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
        this.reducerMap.remove(key);
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

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {
        this.objectMap.keySet().stream().filter(key -> !sideEffects.keys().contains(key)).forEach(key -> sideEffects.set(key, this.objectMap.get(key)));
        this.supplierMap.keySet().stream().forEach(key -> sideEffects.register(key, this.supplierMap.get(key), this.reducerMap.get(key)));
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
            sideEffects.reducerMap = new HashMap<>(this.reducerMap);
            return sideEffects;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    //////////

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public void registerSupplier(final String key, final Supplier supplier) {
        this.register(key, supplier, Operator.assign);

    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public void registerSupplierIfAbsent(final String key, final Supplier supplier) {
        if (!this.supplierMap.containsKey(key)) {
            this.register(key, supplier, Operator.assign);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Deprecated
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return Optional.ofNullable(this.supplierMap.get(key));
    }


}
