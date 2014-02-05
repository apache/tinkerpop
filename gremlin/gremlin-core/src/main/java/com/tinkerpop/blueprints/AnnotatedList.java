package com.tinkerpop.blueprints;

import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.StreamFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Sridhar Ramachandran (lambdazen@gmail.com)
 */
public interface AnnotatedList<V> {

    public static final Object MAKE = new Object();

    public AnnotatedValue<V> addValue(final V value, final Object... annotationKeyValues);

    public AnnotatedListQuery<V> query();

    public default AnnotatedValue<V> get(final int index) {
        final AtomicReference<AnnotatedValue<V>> value = new AtomicReference<>();
        StreamFactory.stream(this.query().limit(index + 1).annotatedValues()).forEach(value::set);
        return value.get();
    }

    public default AnnotatedValue<V> get(final V value) {
        return StreamFactory.stream(this.query().annotatedValues()).filter(av -> av.getValue().equals(value)).findFirst().get();
    }

    public static Object make() {
        return MAKE;
    }
}
