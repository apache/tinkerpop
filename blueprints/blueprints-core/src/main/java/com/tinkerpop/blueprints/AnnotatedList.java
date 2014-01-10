package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;

import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface AnnotatedList<V> {

    public static final Object MAKE = new Object();

    public AnnotatedValue<V> addValue(final V value, final Object... keyValues);

    public AnnotatedListQuery<V> query();

    public static Object make() {
        return MAKE;
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
