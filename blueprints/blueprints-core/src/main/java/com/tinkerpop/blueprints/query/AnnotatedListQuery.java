package com.tinkerpop.blueprints.query;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Compare;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedListQuery<V> extends Query {

    @Override
    public AnnotatedListQuery<V> has(final String key);

    @Override
    public AnnotatedListQuery<V> hasNot(final String key);

    @Override
    public AnnotatedListQuery<V> has(final String key, final BiPredicate biPredicate, final Object value);

    @Override
    public <T extends Comparable<?>> AnnotatedListQuery<V> interval(final String key, final T startValue, final T endValue);

    @Override
    public AnnotatedListQuery<V> limit(final int limit);

    public Iterable<AnnotatedList.AnnotatedValue<V>> annotatedValues();

    public Iterable<V> values();

    // Defaults

    @Override
    public default AnnotatedListQuery<V> has(final String key, final Object value) {
        return this.has(key, Compare.EQUAL, value);
    }

}
