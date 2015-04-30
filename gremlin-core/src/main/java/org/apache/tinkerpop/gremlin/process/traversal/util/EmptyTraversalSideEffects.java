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
import org.apache.tinkerpop.gremlin.structure.Vertex;

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
    public <V> Optional<V> get(final String key) throws IllegalArgumentException {
       return Optional.empty();
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

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public TraversalSideEffects clone()  {
        return this;
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {

    }

    public static EmptyTraversalSideEffects instance() {
        return INSTANCE;
    }
}
