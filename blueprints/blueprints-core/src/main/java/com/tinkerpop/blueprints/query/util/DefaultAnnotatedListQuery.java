package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.AnnotatedList;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.query.AnnotatedListQuery;
import com.tinkerpop.blueprints.util.Pair;

import java.util.function.BiPredicate;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultAnnotatedListQuery extends DefaultQuery implements AnnotatedListQuery {

    public final AnnotatedList annotatedList;

    public DefaultAnnotatedListQuery(final AnnotatedList annotatedList) {
        this.annotatedList = annotatedList;
    }

    public AnnotatedListQuery has(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, value));
        return this;
    }

    public AnnotatedListQuery hasNot(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, value));
        return this;
    }

    public AnnotatedListQuery hasNot(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, null));
        return this;
    }

    public AnnotatedListQuery has(final String key) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, null));
        return this;
    }

    public AnnotatedListQuery has(final String key, final BiPredicate biPredicate, final Object value) {
        this.hasContainers.add(new HasContainer(key, biPredicate, value));
        return this;
    }

    public <T extends Comparable<?>> AnnotatedListQuery interval(final String key, final T startValue, final T endValue) {
        this.hasContainers.add(new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue));
        this.hasContainers.add(new HasContainer(key, Compare.LESS_THAN, endValue));
        return this;
    }

    public AnnotatedListQuery limit(final int limit) {
        this.limit = limit;
        return this;
    }

    public abstract <V> Iterable<Pair<V, AnnotatedList.Annotations>> values();


}
