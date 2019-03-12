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
import org.apache.tinkerpop.machine.coefficients.Coefficient;
import org.apache.tinkerpop.machine.coefficients.LongCoefficient;
import org.apache.tinkerpop.machine.traversers.Path;
import org.apache.tinkerpop.machine.traversers.Traverser;

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

    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal) {
        this.bytecode.lastInstruction().addArg(byTraversal);
        return this;
    }

    public Traversal<C, S, E> c(final C coefficient) {
        this.currentCoefficient.set(coefficient);
        return this;
    }

    public Traversal<C, S, Long> count() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.COUNT);
        return (Traversal) this;
    }

    public Traversal<C, S, E> filter(final Traversal<C, E, ?> filterTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.FILTER, filterTraversal);
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
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY, keyTraversal);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final V value) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, key, value);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final V value) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, keyTraversal, value);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final Traversal<C, Map<K, V>, V> valueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, key, valueTraversal);
        return (Traversal) this;
    }

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final Traversal<C, Map<K, V>, V> valueTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.HAS_KEY_VALUE, keyTraversal, valueTraversal);
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
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.IS, objectTraversal);
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

    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.MAP, mapTraversal);
        return (Traversal) this;
    }

    public Traversal<C, S, Path> path() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.PATH);
        return (Traversal) this;
    }

    public Traversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.REPEAT, repeatTraversal);
        return this;
    }

    public <R extends Number> Traversal<C, S, R> sum() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.SUM);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> unfold() {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.UNFOLD);
        return (Traversal) this;
    }

    public <R> Traversal<C, S, R> union(final Traversal<C, E, R> traversal, Traversal<C, E, R>... traversals) {
        this.bytecode.addInstruction(this.currentCoefficient, Symbols.UNION, traversal, traversals);
        return (Traversal) this;
    }

    public Traversal<C, S, E> until(final Traversal<C, E, ?> untilTraversal) {
        this.bytecode.lastInstruction().addArg(untilTraversal);
        return this;
    }

    ///////

    private final void prepareTraversal() {
        if (null == this.compilation)
            this.compilation = Compilation.compile(this.bytecode);
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
