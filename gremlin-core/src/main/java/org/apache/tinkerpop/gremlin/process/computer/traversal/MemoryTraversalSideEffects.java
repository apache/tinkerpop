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

import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.ProgramPhase;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MemoryTraversalSideEffects implements TraversalSideEffects {

    private TraversalSideEffects sideEffects;
    private Memory memory;
    private ProgramPhase phase;

    private MemoryTraversalSideEffects() {
        // for serialization
    }

    public MemoryTraversalSideEffects(final TraversalSideEffects sideEffects) {
        this.sideEffects = sideEffects;
        this.memory = null;
    }

    public TraversalSideEffects getSideEffects() {
        return this.sideEffects;
    }

    @Override
    public void set(final String key, final Object value) {
        this.sideEffects.set(key, value);
        if (null != this.memory)
            this.memory.set(key, value);
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        return (null != this.memory && this.memory.exists(key)) ? this.memory.get(key) : this.sideEffects.get(key);
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
        if (this.phase.workerState())
            this.memory.add(key, value);
        else
            this.memory.set(key, this.sideEffects.getReducer(key).apply(this.memory.get(key), value));
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
            final MemoryTraversalSideEffects clone = (MemoryTraversalSideEffects) super.clone();
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

    public void storeSideEffectsInMemory() {
        if (this.phase.workerState())
            this.sideEffects.forEach(this.memory::add);
        else
            this.sideEffects.forEach(this.memory::set);
    }

    public static void setMemorySideEffects(final Traversal.Admin<?, ?> traversal, final Memory memory, final ProgramPhase phase) {
        final TraversalSideEffects sideEffects = traversal.getSideEffects();
        if (!(sideEffects instanceof MemoryTraversalSideEffects)) {
            traversal.setSideEffects(new MemoryTraversalSideEffects(sideEffects));
        }
        final MemoryTraversalSideEffects memoryTraversalSideEffects = ((MemoryTraversalSideEffects) traversal.getSideEffects());
        memoryTraversalSideEffects.memory = memory;
        memoryTraversalSideEffects.phase = phase;
    }

    public static ProgramPhase getMemorySideEffectsPhase(final Traversal.Admin<?, ?> traversal) {
        return traversal.getSideEffects() instanceof MemoryTraversalSideEffects ?
                ((MemoryTraversalSideEffects)traversal.getSideEffects()).phase:null;
    }

    public static Set<MemoryComputeKey> getMemoryComputeKeys(final Traversal.Admin<?, ?> traversal) {
        final Set<MemoryComputeKey> keys = new HashSet<>();
        final TraversalSideEffects sideEffects =
                traversal.getSideEffects() instanceof MemoryTraversalSideEffects ?
                        ((MemoryTraversalSideEffects) traversal.getSideEffects()).getSideEffects() :
                        traversal.getSideEffects();
        sideEffects.keys().
                stream().
                forEach(key -> keys.add(MemoryComputeKey.of(key, sideEffects.getReducer(key), true, false)));
        return keys;
    }
}
