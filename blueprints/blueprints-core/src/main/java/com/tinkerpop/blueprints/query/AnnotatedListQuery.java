package com.tinkerpop.blueprints.query;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.util.Pair;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface AnnotatedListQuery extends Query {

    public AnnotatedListQuery has(final String key);

    public AnnotatedListQuery hasNot(final String key);

    public AnnotatedListQuery has(final String key, final BiPredicate biPredicate, final Object value);

    public <T extends Comparable<?>> AnnotatedListQuery interval(final String key, final T startValue, final T endValue);

    public AnnotatedListQuery limit(final int limit);

    public <V> Iterable<Pair<V, AnnotatedList.Annotations>> values();

}
