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

import org.apache.tinkerpop.machine.traverser.Traverser;
import org.apache.tinkerpop.machine.traverser.path.Path;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Traversal<C, S, E> extends Iterator<E> {

    public Traversal<C, S, E> as(final String label);

    public Traversal<C, S, E> barrier();

    public Traversal<C, S, E> by(final Traversal<C, ?, ?> byTraversal);

    public Traversal<C, S, E> by(final String byString);

    public Traversal<C, S, E> c(final C coefficient);

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal, final Traversal<C, E, R> falseTraversal);

    public <R> Traversal<C, S, R> choose(final Traversal<C, E, ?> predicate, final Traversal<C, E, R> trueTraversal);

    public <R> Traversal<C, S, R> constant(final R constant);

    public Traversal<C, S, Long> count();

    public Traversal<C, S, E> emit();

    public Traversal<C, S, E> emit(final Traversal<C, ?, ?> emitTraversal); // TODO: why not <C,E,?>

    public Traversal<C, S, String> explain();

    public Traversal<C, S, E> filter(final Traversal<C, E, ?> filterTraversal);

    public Traversal<C, S, Map<E, Long>> groupCount();

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final P<K> predicate);

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final K key);

    public <K, V> Traversal<C, S, Map<K, V>> hasKey(final Traversal<C, Map<K, V>, K> keyTraversal);

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final V value);

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final V value);

    public <K, V> Traversal<C, S, Map<K, V>> has(final K key, final Traversal<C, Map<K, V>, V> valueTraversal);

    public <K, V> Traversal<C, S, Map<K, V>> has(final Traversal<C, Map<K, V>, K> keyTraversal, final Traversal<C, Map<K, V>, V> valueTraversal);

    public Traversal<C, S, E> identity();

    public Traversal<C, S, E> is(final E object);

    public Traversal<C, S, E> is(final Traversal<C, E, ?> objectTraversal);

    public Traversal<C, S, E> is(final P<E> predicate);

    public Traversal<C, S, Long> incr();

    public Traversal<C, S, Integer> loops();

    public <R> Traversal<C, S, R> map(final Traversal<C, E, R> mapTraversal);

    public Traversal<C, S, Path> path(final String... labels);

    public Traversal<C, S, E> repeat(final Traversal<C, E, E> repeatTraversal);

    public <R extends Number> Traversal<C, S, R> sum();

    public Traversal<C, S, E> times(final int times);

    public <R> Traversal<C, S, R> unfold();

    public <R> Traversal<C, S, R> union(final Traversal<C, E, R>... traversals);

    public Traversal<C, S, E> until(final Traversal<C, ?, ?> untilTraversal); // TODO: why not <C,E,?>

    public <K, V> Traversal<C, S, V> value(final K key);

    /**
     * UTILITY METHODS
     */

    public List<E> toList();

    public Traverser<C, E> nextTraverser();

}
