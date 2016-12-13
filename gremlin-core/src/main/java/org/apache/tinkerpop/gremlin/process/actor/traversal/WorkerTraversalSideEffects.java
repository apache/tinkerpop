/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.process.actor.traversal;

import org.apache.tinkerpop.gremlin.process.actor.Actor;
import org.apache.tinkerpop.gremlin.process.actor.traversal.message.SideEffectAddMessage;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;

import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WorkerTraversalSideEffects implements TraversalSideEffects {

    private TraversalSideEffects sideEffects;
    private Actor.Worker worker;


    private WorkerTraversalSideEffects() {
        // for serialization
    }

    public WorkerTraversalSideEffects(final TraversalSideEffects sideEffects, final Actor.Worker worker) {
        this.sideEffects = sideEffects;
        this.worker = worker;
    }

    public TraversalSideEffects getSideEffects() {
        return this.sideEffects;
    }

    @Override
    public void set(final String key, final Object value) {
        this.sideEffects.set(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        return this.sideEffects.get(key);
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
    public void add(final String key, final Object value) {
        this.sideEffects.add(key, value);
        this.worker.send(this.worker.master(), new SideEffectAddMessage(key, value));
    }

    @Override
    public <V> void register(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {
        this.sideEffects.register(key, initialValue, reducer);
    }

    @Override
    public <V> void registerIfAbsent(final String key, final Supplier<V> initialValue, final BinaryOperator<V> reducer) {
        this.sideEffects.registerIfAbsent(key, initialValue, reducer);
    }

    @Override
    public <V> BinaryOperator<V> getReducer(final String key) {
        return this.sideEffects.getReducer(key);
    }

    @Override
    public <V> Supplier<V> getSupplier(final String key) {
        return this.sideEffects.getSupplier(key);
    }

    @Override
    @Deprecated
    public void registerSupplier(final String key, final Supplier supplier) {
        this.sideEffects.registerSupplier(key, supplier);
    }

    @Override
    @Deprecated
    public <V> Optional<Supplier<V>> getRegisteredSupplier(final String key) {
        return this.sideEffects.getRegisteredSupplier(key);
    }

    @Override
    public <S> void setSack(final Supplier<S> initialValue, final UnaryOperator<S> splitOperator, final BinaryOperator<S> mergeOperator) {
        this.sideEffects.setSack(initialValue, splitOperator, mergeOperator);
    }

    @Override
    public <S> Supplier<S> getSackInitialValue() {
        return this.sideEffects.getSackInitialValue();
    }

    @Override
    public <S> UnaryOperator<S> getSackSplitter() {
        return this.sideEffects.getSackSplitter();
    }

    @Override
    public <S> BinaryOperator<S> getSackMerger() {
        return this.sideEffects.getSackMerger();
    }

    @Override
    public TraversalSideEffects clone() {
        try {
            final WorkerTraversalSideEffects clone = (WorkerTraversalSideEffects) super.clone();
            clone.sideEffects = this.sideEffects.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void mergeInto(final TraversalSideEffects sideEffects) {
        this.sideEffects.mergeInto(sideEffects);
    }

}
