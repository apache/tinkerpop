package com.tinkerpop.blueprints.util;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class StreamFactory {

    public static <T> Stream<T> stream(final Iterable<T> iterable) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.IMMUTABLE), false);
    }

    public static <T> Stream<T> parallelStream(final Iterable<T> iterable) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterable.iterator(), Spliterator.IMMUTABLE), true);
    }
}
