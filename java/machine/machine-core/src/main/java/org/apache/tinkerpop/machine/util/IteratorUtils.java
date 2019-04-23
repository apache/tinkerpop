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
package org.apache.tinkerpop.machine.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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

    public static final <S extends Collection<T>, T> S fill(final Iterator<T> iterator, final S collection) {
        while (iterator.hasNext()) {
            collection.add(iterator.next());
        }
        return collection;
    }

    public static void iterate(final Iterator iterator) {
        while (iterator.hasNext()) {
            iterator.next();
        }
    }

    public static final long count(final Iterator iterator) {
        long ix = 0;
        for (; iterator.hasNext(); ++ix) iterator.next();
        return ix;
    }

    public static <S> Iterator<S> onLast(final Iterator<S> iterator, final Runnable onLast) {
        return new Iterator<>() {
            boolean lastExecuted = false;

            @Override
            public boolean hasNext() {
                final boolean hasNext = iterator.hasNext();
                if (!hasNext && !this.lastExecuted) {
                    this.lastExecuted = true;
                    onLast.run();
                }
                return hasNext;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                try {
                    return iterator.next();
                } finally {
                    if (!iterator.hasNext() && !this.lastExecuted) {
                        this.lastExecuted = true;
                        onLast.run();
                    }
                }
            }
        };
    }

    public static long count(final Iterable iterable) {
        return IteratorUtils.count(iterable.iterator());
    }

    public static <S> List<S> list(final Iterator<S> iterator) {
        return fill(iterator, new ArrayList<>());
    }

    public static <S> List<S> list(final Iterator<S> iterator, final Comparator comparator) {
        final List<S> l = list(iterator);
        Collections.sort(l, comparator);
        return l;
    }

    public static <S> Set<S> set(final Iterator<S> iterator) {
        return fill(iterator, new HashSet<>());
    }

    public static <S> Iterator<S> limit(final Iterator<S> iterator, final int limit) {
        return new Iterator<S>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return iterator.hasNext() && this.count < limit;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                if (this.count++ >= limit)
                    throw FastNoSuchElementException.instance();
                return iterator.next();
            }
        };
    }

    ///////////////////

    public static <T> boolean allMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while (iterator.hasNext()) {
            if (!predicate.test(iterator.next())) {
                return false;
            }
        }

        return true;
    }

    public static <T> boolean anyMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while (iterator.hasNext()) {
            if (predicate.test(iterator.next())) {
                return true;
            }
        }

        return false;
    }

    public static <T> boolean noneMatch(final Iterator<T> iterator, final Predicate<T> predicate) {
        while (iterator.hasNext()) {
            if (predicate.test(iterator.next())) {
                return false;
            }
        }

        return true;
    }

    ///////////////////

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

    public static <S> S reduce(final Iterator<S> iterator, final S identity, final BinaryOperator<S> accumulator) {
        S result = identity;
        while (iterator.hasNext()) {
            result = accumulator.apply(result, iterator.next());
        }

        return result;
    }

    public static <S> S reduce(final Iterable<S> iterable, final S identity, final BinaryOperator<S> accumulator) {
        return reduce(iterable.iterator(), identity, accumulator);
    }

    public static <S, E> E reduce(final Iterator<S> iterator, final E identity, final BiFunction<E, S, E> accumulator) {
        E result = identity;
        while (iterator.hasNext()) {
            result = accumulator.apply(result, iterator.next());
        }

        return result;
    }

    public static <S, E> E reduce(final Iterable<S> iterable, final E identity, final BiFunction<E, S, E> accumulator) {
        return reduce(iterable.iterator(), identity, accumulator);
    }

    ///////////////

    public static final <S> Iterator<S> consume(final Iterator<S> iterator, final Consumer<S> consumer) {
        return new Iterator<S>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public S next() {
                final S s = iterator.next();
                consumer.accept(s);
                return s;
            }
        };
    }

    public static final <S> Iterable<S> consume(final Iterable<S> iterable, final Consumer<S> consumer) {
        return () -> IteratorUtils.consume(iterable.iterator(), consumer);
    }


    ///////////////

    public static final <S, E> Iterator<E> map(final Iterator<S> iterator, final Function<S, E> function) {
        return new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                return function.apply(iterator.next());
            }
        };
    }

    public static final <S, E> Iterable<E> map(final Iterable<S> iterable, final Function<S, E> function) {
        return () -> IteratorUtils.map(iterable.iterator(), function);
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
            public void remove() {
                iterator.remove();
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
        return () -> IteratorUtils.filter(iterable.iterator(), predicate);
    }

    ///////////////////

    public static final <S, E> Iterator<E> flatMap(final Iterator<S> iterator, final Function<S, Iterator<E>> function) {
        return new Iterator<E>() {

            private Iterator<E> currentIterator = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (this.currentIterator.hasNext())
                    return true;
                else {
                    while (iterator.hasNext()) {
                        this.currentIterator = function.apply(iterator.next());
                        if (this.currentIterator.hasNext())
                            return true;
                    }
                }
                return false;
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public E next() {
                if (this.hasNext())
                    return this.currentIterator.next();
                else
                    throw FastNoSuchElementException.instance();
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

    ///////////////////

    public static Iterator asIterator(final Object o) {
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
            itty = of(((Throwable) o).getMessage());
        else
            itty = of(o);
        return itty;
    }

    public static List asList(final Object o) {
        return list(asIterator(o));
    }

    /**
     * Construct a {@link Stream} from an {@link Iterator}.
     */
    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.SIZED), false);
    }

    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return IteratorUtils.stream(iterable.iterator());
    }

    public static <T> Iterator<T> noRemove(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                // do nothing
            }

            @Override
            public T next() {
                return iterator.next();
            }
        };
    }

    public static <T> Iterator<T> removeOnNext(final Iterator<T> iterator) {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public void remove() {
                iterator.remove();
            }

            @Override
            public T next() {
                final T object = iterator.next();
                iterator.remove();
                return object;
            }
        };
    }
}
