package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedList<V> {

    public void add(final V value, final Object... keyValues);

    public Iterator<AnnotatedValue<V>> iterator();

    public Iterator<V> valueIterator();

    public AnnotatedListQuery query();

    public default void forEach(final BiConsumer<V, Annotations> consumer) {
        final Iterator<AnnotatedValue<V>> itty = this.iterator();
        while (itty.hasNext()) {
            final AnnotatedValue<V> value = itty.next();
            consumer.accept(value.getValue(), value.getAnnotations());
        }
    }

    public static <V> AnnotatedList of(final V... values) {
        return new DefaultAnnotatedList<>(Arrays.asList(values));
    }

    public interface Annotations extends Map<String, Optional<Object>> {

    }

    public interface AnnotatedValue<V> {

        public V getValue();

        public Annotations getAnnotations();
    }

}
