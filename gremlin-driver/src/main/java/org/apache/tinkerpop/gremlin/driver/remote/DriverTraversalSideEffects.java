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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverTraversalSideEffects implements TraversalSideEffects {

    private final ResultSet rs;
    private CompletableFuture<Map<String, Object>> future = null;

    public DriverTraversalSideEffects(final ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        initializeFuture();
        try {
            return (V) future.get().get(key);
        } catch (Exception ex) {
            // TODO: PREEEEEEEEEEETTTTTY DUMPY
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void set(String key, Object value) throws IllegalArgumentException {

    }

    @Override
    public void remove(String key) {

    }

    @Override
    public Set<String> keys() {
        initializeFuture();
        try {
            return future.get().keySet();
        } catch (Exception ex) {
            // TODO: PREEEEEEEEEEETTTTTY DUMPY
            throw new RuntimeException(ex);
        }
    }

    @Override
    public <V> void register(String key, Supplier<V> initialValue, BinaryOperator<V> reducer) {

    }

    @Override
    public <V> void registerIfAbsent(String key, Supplier<V> initialValue, BinaryOperator<V> reducer) {

    }

    @Override
    public <V> BinaryOperator<V> getReducer(String key) throws IllegalArgumentException {
        return null;
    }

    @Override
    public <V> Supplier<V> getSupplier(String key) throws IllegalArgumentException {
        return null;
    }

    @Override
    public void add(String key, Object value) throws IllegalArgumentException {

    }

    @Override
    public <S> void setSack(Supplier<S> initialValue, UnaryOperator<S> splitOperator, BinaryOperator<S> mergeOperator) {

    }

    @Override
    public <S> Supplier<S> getSackInitialValue() {
        return null;
    }

    @Override
    public <S> UnaryOperator<S> getSackSplitter() {
        return null;
    }

    @Override
    public <S> BinaryOperator<S> getSackMerger() {
        return null;
    }

    @Override
    public TraversalSideEffects clone() {
        return null;
    }

    @Override
    public void mergeInto(TraversalSideEffects sideEffects) {

    }

    @Override
    public void registerSupplier(String key, Supplier supplier) {

    }

    @Override
    public <V> Optional<Supplier<V>> getRegisteredSupplier(String key) {
        return null;
    }

    @Override
    public String toString() {
        return StringFactory.traversalSideEffectsString(this);
    }

    private synchronized void initializeFuture() {
        if (null == future) future = rs.getSideEffectResults();
    }
}
