package com.tinkerpop.gremlin;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class IteratorUtil {

    public static int count(final Iterator<?> iterator) {
        int counter = 0;
        while (iterator.hasNext()) {
            counter++;
        }

        return counter;
    }

    public static <E> List<E> toList(final Iterator<? extends E> iterator) {
        return toList(iterator, 10);
    }

    public static <E> List<E> toList(final Iterator<? extends E> iterator, final int estimatedSize) {
        if (iterator == null) throw new NullPointerException("Iterator must not be null");
        if (estimatedSize < 1) throw new IllegalArgumentException("Estimated size must be greater than 0");

        final List<E> list = new ArrayList<>(estimatedSize);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
