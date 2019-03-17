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
package org.apache.tinkerpop.language;

import org.apache.tinkerpop.machine.bytecode.Bytecode;
import org.apache.tinkerpop.machine.bytecode.BytecodeUtil;
import org.apache.tinkerpop.machine.bytecode.Compilation;
import org.apache.tinkerpop.machine.bytecode.Instruction;
import org.apache.tinkerpop.machine.bytecode.Symbols;
import org.apache.tinkerpop.machine.coefficient.Coefficient;
import org.apache.tinkerpop.machine.coefficient.LongCoefficient;
import org.apache.tinkerpop.machine.traverser.Path;
import org.apache.tinkerpop.machine.traverser.Traverser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class Traversal<C, S, E> implements Iterator<E> {

    protected final Bytecode<C> bytecode;
    private Coefficient<C> currentCoefficient;
    private Compilation<C, S, E> compilation;
    //
    private long lastCount = 0L;
    private E lastObject = null;

    protected Traversal(final Bytecode<C> bytecode) {
        this.bytecode = bytecode;
        this.currentCoefficient = BytecodeUtil.getCoefficient(this.bytecode).orElse((Coefficient<C>) LongCoefficient.create());
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

    public <R> Traversal<C, S, E> by(final R byObject) {
        this.bytecode.lastInstruction().addArg(byObject);
        return this;
    }

    public Traversal<C, S, E> c(final C coefficient) {
        this.currentCoefficient.set(coefficient);
        return this;
    }

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, S, R> trueTraversal, final Traversal<C, S, R> falseTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.CHOOSE_IF_THEN_ELSE, predicate.bytecode, trueTraversal.bytecode, falseTraversal.bytecode);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, S, R> trueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.CHOOSE_IF_THEN, predicate.bytecode, trueTraversal.bytecode);
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
        return this.emit(__.constant(true));
    }

    public Traversal<C, S, E> emit(final Traversal<C, E, ?> emitTraversal) {
        final Instruction<C> lastInstruction = this.bytecode.lastInstruction();
        if (lastInstruction.op().equals(Symbols.REPEAT))
            lastInstruction.addArgs('e', emitTraversal.bytecode);
        else
            this.bytecode.addInstruction(this.currentCoefficient, Symbols.REPEAT, 'e', emitTraversal.bytecode);
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

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, key);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, keyTraversal.bytecode);
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
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, object);
        return this;
    }

    public Traversal<C, S, E> is(final Traversal<C, E, E> objectTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, objectTraversal.bytecode);
        return this;
    }

    public Traversal<C, S, Long> incr() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.INCR);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> inject(final R... objects) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.INJECT, objects);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> join(Symbols.Tokens joinType, final Traversal<?, ?, Map<K, V>> joinTraversal) {
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
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH);
        return (Traversal) this;
    }

    public Traversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal) {
        final Instruction<C> lastInstruction = this.bytecode.lastInstruction();
        if (lastInstruction.op().equals(Symbols.REPEAT))
            lastInstruction.addArgs('r', repeatTraversal.bytecode);
        else
            this.bytecode.addInstruction(this.currentCoefficient, Symbols.REPEAT, 'r', repeatTraversal.bytecode);
        return this;
    }

    public <R extends Number> Traversal<C, S, R> sum() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.SUM);
        return (Traversal) this;
    }

    public Traversal<C, S, E> times(final int times) {
        return this.until(__.<C, E>loops().is(times)); // TODO: make an int argument
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

    public Traversal<C, S, E> until(final Traversal<C, E, ?> untilTraversal) {
        final Instruction<C> lastInstruction = this.bytecode.lastInstruction();
        if (lastInstruction.op().equals(Symbols.REPEAT))
            lastInstruction.addArgs('u', untilTraversal.bytecode);
        else
            this.bytecode.addInstruction(this.currentCoefficient, Symbols.REPEAT, 'u', untilTraversal.bytecode);
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
