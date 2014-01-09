package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;
import com.tinkerpop.blueprints.util.Pair;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedList<V> {

    public Pair<V, Annotations> get(int index);

    public void add(final V value);

    public void add(final V value, final Object... annotationKeyValues);

    public Iterator<Pair<V, Annotations>> iterator();

    public Iterator<V> valueIterator();

    public AnnotatedListQuery query();

    public default void forEach(final BiConsumer<V, Annotations> consumer) {
        final Iterator<Pair<V, Annotations>> itty = this.iterator();
        while (itty.hasNext()) {
            final Pair<V, Annotations> pair = itty.next();
            consumer.accept(pair.getA(), pair.getB());
        }
    }

    public static <V> AnnotatedList of(final V... values) {
        return new DefaultAnnotatedList(Arrays.asList(values));
    }

    public interface Annotations extends Map<String, Optional<Object>> {

    }

}
