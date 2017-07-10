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

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
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
    public void set(final String key, final Object value) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }

    @Override
    public void remove(final String key) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }

    @Override
    public Set<String> keys() {
        return Collections.emptySet();
    }

    @Override
    public void add(final String key, final Object value) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }

    @Override
    public <V> void register(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {

    }

    @Override
    public <V> void registerIfAbsent(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {

    }

    @Override
    public <V> BinaryOperator<V> getReducer(final String key) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }


    @Override
    public <V> Supplier<V> getSupplier(final String key) throws IllegalArgumentException {
        throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final UnaryOperator<S> splitOperator, final BinaryOperator<S> mergeOperator) {

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

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public TraversalSideEffects clone() {
        return this;
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {

    }

    public static EmptyTraversalSideEffects instance() {
        return INSTANCE;
    }
}
