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
package org.apache.tinkerpop.language.gremlin;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.bytecode.CoreCompiler.Symbols;
import org.apache.tinkerpop.machine.bytecode.Pred;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.path.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traversal<C, S, E> implements Iterator<E> {

    protected final Bytecode<C> bytecode;
    private Compilation<C, S, E> compilation;
    private Coefficient<C> currentCoefficient;

    // iteration helpers
    private long lastCount = 0L;
    private E lastObject = null;

    // used by __
    Traversal() {
        this.bytecode = new Bytecode<>();
        this.currentCoefficient = BytecodeUtil.getCoefficient(this.bytecode).orElse((Coefficient<C>) LongCoefficient.create()); // TODO: this will cause __ problems
    }

    // used by TraversalSource
    Traversal(final Coefficient<C> unity, final Bytecode<C> bytecode) {
        this.bytecode = bytecode;
        this.currentCoefficient = unity;
    }

    public Traversal<C, S, E> as(final String label) {
        this.bytecode.lastInstruction().addLabel(label);
        return this;
    }

    public Traversal<C, S, E> barrier() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.BARRIER);
        return this;
    }

    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(byTraversal.bytecode);
        return this;
    }

    public Traversal<C, S, E> by(final String byString) {
        this.bytecode.lastInstruction().addArg(byString);
        return this;
    }

    public Traversal<C, S, E> c(final C coefficient) {
        this.currentCoefficient.set(coefficient);
        return this;
    }

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, S, R> trueTraversal, final Traversal<C, S, R> falseTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IF, predicate.bytecode, trueTraversal.bytecode, falseTraversal.bytecode);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, S, R> trueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IF, predicate.bytecode, trueTraversal.bytecode);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> constant(final R constant) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.CONSTANT, constant);
        return (Traversal) this;
    }

    public Traversal<C, S, Long> count() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.COUNT);
        return (Traversal) this;
    }

    public Traversal<C, S, E> emit() {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'e', true);
        return this;
    }

    public Traversal<C, S, E> emit(final Traversal<C, ?, ?> emitTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'e', emitTraversal.bytecode);
        return this;
    }

    public Traversal<C, S, E> filter(final Traversal<C, E, ?> filterTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, filterTraversal.bytecode);
        return this;
    }

    public Traversal<C, S, Map<E, Long>> groupCount() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.GROUP_COUNT);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final P<K> predicate) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final K key) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, Pred.eq.name(), key);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final Traversal<C, Map<K, V>, K> keyTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, Pred.eq.name(), keyTraversal.bytecode);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final V value) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, key, value);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final V value) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, keyTraversal.bytecode, value);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final Traversal<C, Map<K, V>, V> valueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, key, valueTraversal.bytecode);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final Traversal<C, Map<K, V>, V> valueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, keyTraversal.bytecode, valueTraversal.bytecode);
        return (Traversal) this;
    }

    public Traversal<C, S, E> identity() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IDENTITY);
        return this;
    }

    public Traversal<C, S, E> is(final E object) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, Pred.eq.name(), object);
        return this;
    }

    public Traversal<C, S, E> is(final Traversal<C, E, E> objectTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, Pred.eq.name(), objectTraversal.bytecode);
        return this;
    }

    public Traversal<C, S, E> is(final P<E> predicate) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, predicate.type().name(), TraversalUtil.tryToGetBytecode(predicate.object()));
        return this;
    }

    public Traversal<C, S, Long> incr() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.INCR);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> join(final Symbols.Tokens joinType, final Traversal<?, ?, Map<K, V>> joinTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.JOIN, joinType, joinTraversal.bytecode);
        return (Traversal) this;
    }

    public Traversal<C, S, Integer> loops() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.LOOPS);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, mapTraversal.bytecode);
        return (Traversal) this;
    }

    public Traversal<C, S, Path> path() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH, Collections.emptyList());
        return (Traversal) this;
    }

    public Traversal<C, S, Path> path(final String label, final String... labels) {
        final List<String> asLabels = new ArrayList<>(labels.length + 1);
        asLabels.add(label);
        Collections.addAll(asLabels, labels);
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH, asLabels);
        return (Traversal) this;
    }

    public Traversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'r', repeatTraversal.bytecode);
        return this;
    }

    public <R extends Number> Traversal<C, S, R> sum() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.SUM);
        return (Traversal) this;
    }

    public Traversal<C, S, E> times(final int times) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'u', times);
        return this;
    }

    public <R> Traversal<C, S, R> unfold() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.UNFOLD);
        return (Traversal) this;
    }

    // TODO: for some reason var args are not working...Java11

    public <R> Traversal<C, S, R> union(final Traversal<C, E, R> traversalA, final Traversal<C, E, R> traversalB) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.UNION, traversalA.bytecode, traversalB.bytecode);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> union(final Traversal<C, E, R> traversalA, final Traversal<C, E, R> traversalB, final Traversal<C, E, R> traversalC) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.UNION, traversalA.bytecode, traversalB.bytecode, traversalC.bytecode);
        return (Traversal) this;
    }

    public Traversal<C, S, E> until(final Traversal<C, ?, ?> untilTraversal) {
        TraversalUtil.insertRepeatInstruction(this.bytecode, this.currentCoefficient, 'u', untilTraversal.bytecode);
        return this;
    }

    ///////

    private final void prepareTraversal() {
        if (null == this.compilation)
            this.compilation = Compilation.compile(this.bytecode);
    }

    public Traverser<C, E> nextTraverser() {
        this.prepareTraversal();
        return this.compilation.getProcessor().next(); // TODO: interaction with hasNext/next and counts
    }

    @Override
    public boolean hasNext() {
        this.prepareTraversal();
        return this.lastCount > 0 || this.compilation.getProcessor().hasNext();
    }

    @Override
    public E next() {
        this.prepareTraversal();
        if (this.lastCount > 0) {
            this.lastCount--;
            return this.lastObject;
        } else {
            final Traverser<C, E> traverser = this.compilation.getProcessor().next();
            if (traverser.coefficient().count() > 1) {
                this.lastObject = traverser.object();
                this.lastCount = traverser.coefficient().count() - 1L;
            }
            return traverser.object();
        }
    }

    public List<E> toList() {
        final List<E> list = new ArrayList<>();
        while (this.hasNext()) {
            list.add(this.next());
        }
        return list;
    }

    @Override
    public String toString() {
        this.prepareTraversal();
        return this.compilation.getProcessor().toString();
    }
}
