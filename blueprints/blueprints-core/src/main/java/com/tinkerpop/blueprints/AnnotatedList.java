package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;

import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedList<V> {

    public void add(final V value, final Object... keyValues);

    public AnnotatedListQuery<V> query();

    public static <V> AnnotatedList of(final V... values) {
        return new DefaultAnnotatedList<>(values);
    }

    public static <V> AnnotatedList of(final AnnotatedValue<V>... annotatedValues) {
        return new DefaultAnnotatedList<>(annotatedValues);
    }

    public interface Annotations {

        public void set(final String key, final Object value);

        public <T> Optional<T> get(final String key);

        public Set<String> getKeys();

    }

    public interface AnnotatedValue<V> {

        public V getValue();

        public Annotations getAnnotations();
    }

}
