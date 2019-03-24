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
package org.apache.tinkerpop.language.gremlin.core;

import org.apache.tinkerpop.language.gremlin.AbstractTraversal;
import org.apache.tinkerpop.language.gremlin.P;
import org.apache.tinkerpop.language.gremlin.Traversal;
import org.apache.tinkerpop.language.gremlin.TraversalUtil;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.Oper;
import org.apache.tinkerpop.machine.bytecode.Pred;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.strategy.decoration.ExplainStrategy;
import org.apache.tinkerpop.machine.traverser.path.Path;

import java.util.Arrays;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CoreTraversal<C, S, E> extends AbstractTraversal<C, S, E> {

    // used by __
    CoreTraversal() {
        // TODO: this will cause __ problems
        super(new Bytecode<>(), (Coefficient<C>) LongCoefficient.create());
    }

    // used by TraversalSource
    protected CoreTraversal(final Coefficient<C> unity, final Bytecode<C> bytecode) {
        super(bytecode, unity);
    }

    @Override
    public CoreTraversal<C, S, E> as(final String label) {
        this.bytecode.lastInstruction().addLabel(label);
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> barrier() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.BARRIER);
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(TraversalUtil.getBytecode(byTraversal));
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> by(final String byString) {
        this.bytecode.lastInstruction().addArg(byString);
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> c(final C coefficient) {
        this.currentCoefficient.set(coefficient);
        return this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal, final Traversal<C, E, R> falseTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.BRANCH, TraversalUtil.getBytecode(predicate), TraversalUtil.getBytecode(trueTraversal), Symbols.DEFAULT, TraversalUtil.getBytecode(falseTraversal));
        return (CoreTraversal) this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.BRANCH, TraversalUtil.getBytecode(predicate), TraversalUtil.getBytecode(trueTraversal));
        return (CoreTraversal) this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> constant(final R constant) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, constant);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, Long> count() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, "traverser::count");
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.REDUCE, Oper.sum.name(), 0L);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> emit() {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'e', true);
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> emit(final Traversal<C, ?, ?> emitTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'e', TraversalUtil.getBytecode(emitTraversal));
        return this;
    }

    @Override
    public CoreTraversal<C, S, String> explain() {
        this.bytecode.addSourceInstruction(Symbols.WITH_STRATEGY, ExplainStrategy.class); // TODO: maybe its best to have this in the global cache
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.EXPLAIN);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> filter(final Traversal<C, E, ?> filterTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, TraversalUtil.getBytecode(filterTraversal));
        return this;
    }

    @Override
    public CoreTraversal<C, S, Map<E, Long>> groupCount() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.GROUP_COUNT);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> hasKey(final P<K> predicate) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> hasKey(final K key) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), key);
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> hasKey(final Traversal<C, Map<K, V>, K> keyTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(keyTraversal));
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> has(final K key, final V value) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.tryToGetBytecode(key));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), value);
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final V value) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.getBytecode(keyTraversal));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), value);
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> has(final K key, final Traversal<C, Map<K, V>, V> valueTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", key);
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(valueTraversal));
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final Traversal<C, Map<K, V>, V> valueTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.getBytecode(keyTraversal));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(valueTraversal));
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, internal);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> identity() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, "traverser::object");
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> is(final E object) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.tryToGetBytecode(object));
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> is(final Traversal<C, E, ?> objectTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(objectTraversal));
        return this;
    }

    @Override
    public CoreTraversal<C, S, E> is(final P<E> predicate) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
        return this;
    }

    @Override
    public CoreTraversal<C, S, Long> incr() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, "number::add", 1L);
        return (CoreTraversal) this;
    }

    public <K, V> CoreTraversal<C, S, Map<K, V>> join(final Symbols.Tokens joinType, final CoreTraversal<?, ?, Map<K, V>> joinTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.JOIN, joinType, joinTraversal.bytecode);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, Integer> loops() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, "traverser:loops");
        return (CoreTraversal) this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, TraversalUtil.getBytecode(mapTraversal));
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, Path> path(final String... labels) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH, Arrays.asList(labels));
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'r', TraversalUtil.getBytecode(repeatTraversal));
        return this;
    }

    @Override
    public <R extends Number> CoreTraversal<C, S, R> sum() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.REDUCE, Oper.sum.name(), 0);
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> times(final int times) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'u', times);
        return this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> unfold() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "traverser::object");
        return (CoreTraversal) this;
    }

    @Override
    public <R> CoreTraversal<C, S, R> union(final Traversal<C, E, R>... traversals) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.BRANCH, TraversalUtil.createUnionArguments(traversals));
        return (CoreTraversal) this;
    }

    @Override
    public CoreTraversal<C, S, E> until(final Traversal<C, ?, ?> untilTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'u', TraversalUtil.getBytecode(untilTraversal));
        return this;
    }

    @Override
    public <K, V> CoreTraversal<C, S, V> value(final K key) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", key);
        return (CoreTraversal) this;
    }
}
