package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.DefaultAnnotatedList;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedList<V> {

    public void add(final V value, final Object... keyValues);

    public AnnotatedListQuery<V> query();

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
