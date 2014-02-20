package com.tinkerpop.gremlin.structure.query.util;

import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.query.AnnotatedListQuery;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.function.BiPredicate;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultAnnotatedListQuery<V> extends DefaultQuery implements AnnotatedListQuery<V> {

    public AnnotatedListQuery<V> has(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, value));
        return this;
    }

    public AnnotatedListQuery<V> hasNot(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, value));
        return this;
    }

    public AnnotatedListQuery<V> hasNot(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, null));
        return this;
    }

    public AnnotatedListQuery<V> has(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, null));
        return this;
    }

    public AnnotatedListQuery<V> has(final String key, final BiPredicate biPredicate, final Object value) {
        this.hasContainers.add(new HasContainer(key, biPredicate, value));
        return this;
    }

    public <T extends Comparable<?>> AnnotatedListQuery<V> interval(final String key, final T startValue, final T endValue) {
        this.hasContainers.add(new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue));
        this.hasContainers.add(new HasContainer(key, Compare.LESS_THAN, endValue));
        return this;
    }

    public AnnotatedListQuery<V> limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public abstract Iterable<AnnotatedValue<V>> annotatedValues();

    public abstract Iterable<V> values();
}
