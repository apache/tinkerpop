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
package org.apache.tinkerpop.language.gremlin.common;

import org.apache.tinkerpop.language.gremlin.AbstractTraversal;
import org.apache.tinkerpop.language.gremlin.P;
import org.apache.tinkerpop.language.gremlin.Traversal;
import org.apache.tinkerpop.language.gremlin.TraversalUtil;
import org.apache.tinkerpop.machine.Machine;
import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.compiler.CommonCompiler.Symbols;
import org.apache.tinkerpop.machine.bytecode.compiler.Order;
import org.apache.tinkerpop.machine.bytecode.compiler.Pred;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.structure.data.TMap;
import org.apache.tinkerpop.machine.traverser.path.Path;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class CommonTraversal<C, S, E> extends AbstractTraversal<C, S, E> {

    // used by __
    CommonTraversal() {
        // TODO: this will cause __ problems
        this(null, new Bytecode<>(), (Coefficient<C>) LongCoefficient.create());
    }

    // used by TraversalSource
    public CommonTraversal(final Machine machine, final Bytecode<C> bytecode, final Coefficient<C> unity) {
        super(machine, bytecode, unity);
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
    public Traversal<C, S, E> by(final String byString) {
        this.bytecode.lastInstruction().addArg(byString);
        return this;
    }

    @Override
    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(TraversalUtil.getBytecode(byTraversal));
        return this;
    }

    @Override
    public Traversal<C, S, E> by(final Order order) {
        return this.by(__.identity(), order);
    }

    @Override
    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal, final Order order) {
        this.bytecode.lastInstruction().addArgs(TraversalUtil.getBytecode(byTraversal), order.name());
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
        return this.addInstruction(Symbols.CONSTANT, constant);
    }

    @Override
    public Traversal<C, S, Long> count() {
        return this.addInstruction(Symbols.COUNT);
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
    public <R> Traversal<C, S, R> flatMap(final Traversal<C, E, R> flatMapTraversal) {
        return this.addInstruction(Symbols.FLATMAP, TraversalUtil.getBytecode(flatMapTraversal));
    }

    @Override
    public Traversal<C, S, TMap<E, Long>> groupCount() {
        return this.addInstruction(Symbols.GROUP_COUNT);
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final K key, final V value) {
        return this.addInstruction(Symbols.HAS_KEY_VALUE, TraversalUtil.tryToGetBytecode(key), TraversalUtil.tryToGetBytecode(value));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final Traversal<C, TMap<K, V>, K> keyTraversal, final V value) {
        return this.addInstruction(Symbols.HAS_KEY_VALUE, TraversalUtil.getBytecode(keyTraversal), TraversalUtil.tryToGetBytecode(value));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final K key, final Traversal<C, TMap<K, V>, V> valueTraversal) {
        return this.addInstruction(Symbols.HAS_KEY_VALUE, TraversalUtil.tryToGetBytecode(key), TraversalUtil.getBytecode(valueTraversal));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> has(final Traversal<C, TMap<K, V>, K> keyTraversal, final Traversal<C, TMap<K, V>, V> valueTraversal) {
        return this.addInstruction(Symbols.HAS_KEY_VALUE, TraversalUtil.getBytecode(keyTraversal), TraversalUtil.getBytecode(valueTraversal));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final P<K> predicate) {
        return this.addInstruction(Symbols.HAS_KEY, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final K key) {
        return this.addInstruction(Symbols.HAS_KEY, TraversalUtil.tryToGetBytecode(key));
    }

    @Override
    public <K, V> Traversal<C, S, TMap<K, V>> hasKey(final Traversal<C, TMap<K, V>, K> keyTraversal) {
        return this.addInstruction(Symbols.HAS_KEY, TraversalUtil.getBytecode(keyTraversal));
    }

    @Override
    public Traversal<C, S, E> identity() {
        return this.addInstruction(Symbols.IDENTITY);
    }

    @Override
    public Traversal<C, S, E> is(final E object) {
        return this.addInstruction(Symbols.IS, Pred.eq.name(), TraversalUtil.tryToGetBytecode(object));
    }

    @Override
    public Traversal<C, S, E> is(final Traversal<C, E, ?> objectTraversal) {
        return this.addInstruction(Symbols.IS, Pred.eq.name(), TraversalUtil.getBytecode(objectTraversal));
    }

    @Override
    public Traversal<C, S, E> is(final P<E> predicate) {
        return this.addInstruction(Symbols.IS, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
    }

    @Override
    public Traversal<C, S, Long> incr() {
        return this.addInstruction(Symbols.INCR);
    }

    @Override
    public Traversal<C, S, Integer> loops() {
        return this.addInstruction(Symbols.LOOPS);
    }

    @Override
    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        return this.addInstruction(Symbols.MAP, TraversalUtil.getBytecode(mapTraversal));
    }

    @Override
    public Traversal<C, S, E> order() {
        return this.addInstruction(Symbols.ORDER);
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
        return this.addInstruction(Symbols.SUM);
    }

    @Override
    public Traversal<C, S, E> times(final int times) {
        return TraversalUtil.insertRepeatInstruction(this, 'u', times);
    }

    @Override
    public <R> Traversal<C, S, R> unfold() {
        return this.addInstruction(Symbols.UNFOLD);
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
        return this.addInstruction(Symbols.VALUE, key);
    }
}
