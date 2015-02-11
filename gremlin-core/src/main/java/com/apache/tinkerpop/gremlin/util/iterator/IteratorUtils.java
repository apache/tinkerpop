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
package com.apache.tinkerpop.gremlin.util.iterator;

import com.apache.tinkerpop.gremlin.process.FastNoSuchElementException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class IteratorUtils {

    private IteratorUtils() {

    }

    public static final <S> Iterator<S> of(final S a) {
        return new SingleIterator<>(a);
    }

    public static final <S> Iterator<S> of(final S a, S b) {
        return new DoubleIterator<>(a, b);
    }

    ///////////////

    public static Iterator convertToIterator(final Object o) {
        final Iterator itty;
        if (o instanceof Iterable)
            itty = ((Iterable) o).iterator();
        else if (o instanceof Iterator)
            itty = (Iterator) o;
        else if (o instanceof Object[])
            itty = new ArrayIterator<>((Object[]) o);
        else if (o instanceof Stream)
            itty = ((Stream) o).iterator();
        else if (o instanceof Map)
            itty = ((Map) o).entrySet().iterator();
        else if (o instanceof Throwable)
            itty = IteratorUtils.of(((Throwable) o).getMessage());
        else
            itty = IteratorUtils.of(o);
        return itty;
    }

    public static List convertToList(final Object o) {
        final Iterator iterator = IteratorUtils.convertToIterator(o);
        return list(iterator);
    }

    public static final <S extends Collection<T>, T> S fill(final Iterator<T> iterator, final S collection) {
        while (iterator.hasNext()) {
            collection.add(iterator.next());
        }
        return collection;
    }

    public static final long count(final Iterator iterator) {
        long ix = 0;
        for (; iterator.hasNext(); ++ix) iterator.next();
        return ix;
    }

    public static <S> List<S> list(final Iterator<S> iterator) {
        return fill(iterator, new ArrayList<>());
    }

    public static <K, S> Map<K, S> collectMap(final Iterator<S> iterator, final Function<S, K> key) {
        return collectMap(iterator, key, Function.identity());
    }

    public static <K, S, V> Map<K, V> collectMap(final Iterator<S> iterator, final Function<S, K> key, final Function<S, V> value) {
        final Map<K, V> map = new HashMap<>();
        while (iterator.hasNext()) {
            final S obj = iterator.next();
            map.put(key.apply(obj), value.apply(obj));
        }
        return map;
    }

    public static <K, S> Map<K, List<S>> groupBy(final Iterator<S> iterator, final Function<S, K> groupBy) {
        final Map<K, List<S>> map = new HashMap<>();
        while (iterator.hasNext()) {
            final S obj = iterator.next();
            map.computeIfAbsent(groupBy.apply(obj), k -> new ArrayList<>()).add(obj);
        }
        return map;
    }

    ///////////////

    public static final <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }
        };
    }

    public static final <S, E> Iterable<E> map(final Iterable<S> iterable, final Function<S, E> function) {
        return new Iterable<E>() {
            @Override
            public Iterator<E> iterator() {
                return IteratorUtils.map(iterable.iterator(), function);
            }
        };
    }

    ///////////////

    public static final <S> Iterator<S> filter(final Iterator<S> iterator, final Predicate<S> predicate) {


        return new Iterator<S>() {
            S nextResult = null;

            @Override
            public boolean hasNext() {
                if (null != this.nextResult) {
                    return true;
                } else {
                    advance();
                    return null != this.nextResult;
                }
            }

            @Override
            public S next() {
                try {
                    if (null != this.nextResult) {
                        return this.nextResult;
                    } else {
                        advance();
                        if (null != this.nextResult)
                            return this.nextResult;
                        else
                            throw FastNoSuchElementException.instance();
                    }
                } finally {
                    this.nextResult = null;
                }
            }

            private final void advance() {
                this.nextResult = null;
                while (iterator.hasNext()) {
                    final S s = iterator.next();
                    if (predicate.test(s)) {
                        this.nextResult = s;
                        return;
                    }
                }
            }
        };
    }

    public static final <S> Iterable<S> filter(final Iterable<S> iterable, final Predicate<S> predicate) {
        return new Iterable<S>() {
            @Override
            public Iterator<S> iterator() {
                return IteratorUtils.filter(iterable.iterator(), predicate);
            }
        };
    }

    ///////////////////

    public static final <S> Iterator<S> concat(final Iterator<S>... iterators) {
        final MultiIterator<S> iterator = new MultiIterator<>();
        for (final Iterator<S> itty : iterators) {
            iterator.addIterator(itty);
        }
        return iterator;
    }
}
