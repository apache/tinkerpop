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
import org.apache.tinkerpop.machine.bytecode.compiler.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.bytecode.compiler.Oper;
import org.apache.tinkerpop.machine.bytecode.compiler.Pred;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.structure.data.TMap;
import org.apache.tinkerpop.machine.traverser.path.Path;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CoreTraversal<C, S, E> extends AbstractTraversal<C, S, E> {

    // used by __
    CoreTraversal() {
        // TODO: this will cause __ problems
        this(new Bytecode<>(), (Coefficient<C>) LongCoefficient.create());
    }

    // used by TraversalSource
    public CoreTraversal(final Bytecode<C> bytecode, final Coefficient<C> unity) {
        super(bytecode, unity);
    }

    @Override
    public Traversal<C, S, E> as(final String label) {
        this.bytecode.lastInstruction().setLabel(label);
        return this;
    }

    @Override
    public Traversal<C, S, E> barrier() {
        return this.addInstruction(Symbols.BARRIER);
    }

    @Override
    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(TraversalUtil.getBytecode(byTraversal));
        return this;
    }

    @Override
    public Traversal<C, S, E> by(final String byString) {
        this.bytecode.lastInstruction().addArg(byString);
        return this;
    }

    @Override
    public Traversal<C, S, E> c(final C coefficient) {
        this.currentCoefficient.set(coefficient);
        return this;
    }

    @Override
    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal, final Traversal<C, E, R> falseTraversal) {
        return this.addInstruction(Symbols.BRANCH, TraversalUtil.getBytecode(predicate), TraversalUtil.getBytecode(trueTraversal), Symbols.DEFAULT, TraversalUtil.getBytecode(falseTraversal));
    }

    @Override
    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal) {
        return this.addInstruction(Symbols.BRANCH, TraversalUtil.getBytecode(predicate), TraversalUtil.getBytecode(trueTraversal));
    }

    @Override
    public <R> Traversal<C, S, R> constant(final R constant) {
        return this.addInstruction(Symbols.MAP, constant);
    }

    @Override
    public Traversal<C, S, Long> count() {
        this.addInstruction(Symbols.MAP, "traverser::count");
        return this.addInstruction(Symbols.REDUCE, Oper.sum.name(), 0L);
    }

    @Override
    public Traversal<C, S, E> emit() {
        return TraversalUtil.insertRepeatInstruction(this, 'e', true);
    }

    @Override
    public Traversal<C, S, E> emit(final Traversal<C, ?, ?> emitTraversal) {
        return TraversalUtil.insertRepeatInstruction(this, 'e', TraversalUtil.getBytecode(emitTraversal));
    }

    @Override
    public Traversal<C, S, String> explain() {
        return this.addInstruction(Symbols.EXPLAIN);
    }

    @Override
    public Traversal<C, S, E> filter(final Traversal<C, E, ?> filterTraversal) {
        return this.addInstruction(Symbols.FILTER, TraversalUtil.getBytecode(filterTraversal));
    }

    @Override
    public Traversal<C, S, TMap<E, Long>> groupCount() {
        return this.addInstruction(Symbols.GROUP_COUNT);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final P<K> predicate) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final K key) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), key);
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final Traversal<C, TMap<K, V>, K> keyTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.FLATMAP, "dictionary::keys");
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(keyTraversal));
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final K key, final V value) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.tryToGetBytecode(key));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), value);
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final Traversal<C, TMap<K, V>, K> keyTraversal, final V value) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.getBytecode(keyTraversal));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), value);
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final K key, final Traversal<C, TMap<K, V>, V> valueTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", key);
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(valueTraversal));
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final Traversal<C, TMap<K, V>, K> keyTraversal, final Traversal<C, TMap<K, V>, V> valueTraversal) {
        final Bytecode<C> internal = new Bytecode<>();
        internal.addInstruction(this.currentCoefficient, Symbols.MAP, "dictionary::get", TraversalUtil.getBytecode(keyTraversal));
        internal.addInstruction(this.currentCoefficient, Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(valueTraversal));
        return this.addInstruction(Symbols.FILTER, internal);
    }

    @Override
    public Traversal<C, S, E> identity() {
        return this.addInstruction(Symbols.MAP, "traverser::object");
    }

    @Override
    public Traversal<C, S, E> is(final E object) {
        return this.addInstruction(Symbols.FILTER, Pred.eq.name(), TraversalUtil.tryToGetBytecode(object));
    }

    @Override
    public Traversal<C, S, E> is(final Traversal<C, E, ?> objectTraversal) {
        return this.addInstruction(Symbols.FILTER, Pred.eq.name(), TraversalUtil.getBytecode(objectTraversal));
    }

    @Override
    public Traversal<C, S, E> is(final P<E> predicate) {
        return this.addInstruction(Symbols.FILTER, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
    }

    @Override
    public Traversal<C, S, Long> incr() {
        return this.addInstruction(Symbols.MAP, "number::add", 1L);
    }

    public <K, V> Traversal<C, S, Map<K, V>> join(final Symbols.Tokens joinType, final CoreTraversal<?, ?, Map<K, V>> joinTraversal) {
        return this.addInstruction(Symbols.JOIN, joinType, joinTraversal.bytecode);
    }

    @Override
    public Traversal<C, S, Integer> loops() {
        return this.addInstruction(Symbols.MAP, "traverser:loops");
    }

    @Override
    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        return this.addInstruction(Symbols.MAP, TraversalUtil.getBytecode(mapTraversal));
    }

    @Override
    public Traversal<C, S, Path> path(final String... labels) {
        return this.addInstruction(Symbols.PATH, TraversalUtil.addObjects(labels, "|"));
    }

    @Override
    public Traversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal) {
        return TraversalUtil.insertRepeatInstruction(this, 'r', TraversalUtil.getBytecode(repeatTraversal));
    }

    @Override
    public <R extends Number> Traversal<C, S, R> sum() {
        return this.addInstruction(Symbols.REDUCE, Oper.sum.name(), 0);
    }

    @Override
    public Traversal<C, S, E> times(final int times) {
        return TraversalUtil.insertRepeatInstruction(this, 'u', times);
    }

    @Override
    public <R> Traversal<C, S, R> unfold() {
        return this.addInstruction(Symbols.FLATMAP, "traverser::object");
    }

    @Override
    public <R> Traversal<C, S, R> union(final Traversal<C, E, R>... traversals) {
        return this.addInstruction(Symbols.BRANCH, TraversalUtil.createUnionArguments(traversals));
    }

    @Override
    public Traversal<C, S, E> until(final Traversal<C, ?, ?> untilTraversal) {
        return TraversalUtil.insertRepeatInstruction(this, 'u', TraversalUtil.getBytecode(untilTraversal));
    }

    @Override
    public <K, V> Traversal<C, S, V> value(final K key) {
        return this.addInstruction(Symbols.MAP, "dictionary::get", key);
    }
}
