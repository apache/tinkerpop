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
package org.apache.tinkerpop.gremlin.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for constructing {@link java.util.stream.Stream} objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StreamFactory {

    /**
     * Construct a {@link java.util.stream.Stream} from an {@link Iterable}.
     */
    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return StreamFactory.stream(iterable.iterator());
    }

    /**
     * Construct a parallel {@link java.util.stream.Stream} from an {@link Iterable}.
     */
    public static <T> Stream<T> parallelStream(final Iterable<T> iterable) {
        return StreamFactory.parallelStream(iterable.iterator());
    }

    /**
     * Construct a {@link java.util.stream.Stream} from an {@link java.util.Iterator}.
     */
    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.SIZED), false);
    }

    /**
     * Construct a parallel {@link java.util.stream.Stream} from an {@link java.util.Iterator}.
     */
    public static <T> Stream<T> parallelStream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE | Spliterator.SIZED), true);
    }

    /**
     * Construct an {@link Iterable} from an {@link java.util.stream.Stream}.
     */
    public static <T> Iterable<T> iterable(final Stream<T> stream) {
        return stream::iterator;
    }

    public static <T> Stream<T> stream(final T t) {
        return Stream.of(t);
    }
}
