package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;

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

    public interface AnnotatedValue<V> {

        public V getValue();

        public Annotations getAnnotations();
    }
}
