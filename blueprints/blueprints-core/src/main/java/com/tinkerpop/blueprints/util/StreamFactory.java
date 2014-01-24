package com.tinkerpop.blueprints.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods for constructing {@link Stream} objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StreamFactory {

    /**
     * Construct a {@link Stream} from an {@link Iterable}.
     */
    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return StreamFactory.stream(iterable.iterator());
    }

    /**
     * Construct a parallel {@link Stream} from an {@link Iterable}.
     */
    public static <T> Stream<T> parallelStream(final Iterable<T> iterable) {
        return StreamFactory.parallelStream(iterable.iterator());
    }

    /**
     * Construct a {@link Stream} from an {@link Iterator}.
     */
    public static <T> Stream<T> stream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE), false);
    }

    /**
     * Construct a parallel {@link Stream} from an {@link Iterator}.
     */
    public static <T> Stream<T> parallelStream(final Iterator<T> iterator) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE), true);
    }

    /**
     * Construct an {@link Iterable} from an {@link Stream}.
     */
    public static <T> Iterable<T> iterable(final Stream<T> stream) {
        return () -> stream.iterator();
    }

    public static <T> Stream<T> stream(final T t) {
        return Arrays.asList(t).stream();
    }
}
