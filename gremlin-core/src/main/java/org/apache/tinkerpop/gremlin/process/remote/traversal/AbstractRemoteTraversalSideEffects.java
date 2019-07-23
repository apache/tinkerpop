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
package org.apache.tinkerpop.gremlin.process.remote.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.8, not directly replaced, prefer use of {@link GraphTraversal#cap(String, String...)}
 * to return the result as part of the traversal iteration.
 */
@Deprecated
public abstract class AbstractRemoteTraversalSideEffects implements RemoteTraversalSideEffects {

    @Override
    public void set(final String key, final Object value) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void remove(final String key) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <V> void register(final String key, final Supplier<V> initialValue, BinaryOperator<V> reducer) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <V> void registerIfAbsent(final String key, final Supplier<V> initialValue, BinaryOperator<V> reducer) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <V> BinaryOperator<V> getReducer(final String key) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <V> Supplier<V> getSupplier(final String key) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void add(final String key, final Object value) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, UnaryOperator<S> splitOperator, final BinaryOperator<S> mergeOperator) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S> Supplier<S> getSackInitialValue() {
        return null;
        // throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S> UnaryOperator<S> getSackSplitter() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public <S> BinaryOperator<S> getSackMerger() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public TraversalSideEffects clone() {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {
        throw new UnsupportedOperationException("Remote traversals do not support this method");
    }
}
