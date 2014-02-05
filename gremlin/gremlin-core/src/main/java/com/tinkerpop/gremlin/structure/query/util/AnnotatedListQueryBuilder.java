package com.tinkerpop.gremlin.structure.query.util;

import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.AnnotatedValue;
import com.tinkerpop.gremlin.structure.query.AnnotatedListQuery;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AnnotatedListQueryBuilder<V> extends DefaultAnnotatedListQuery<V> {

    public AnnotatedListQueryBuilder<V> has(final String key, final Object value) {
        super.has(key, value);
        return this;
    }

    public AnnotatedListQueryBuilder<V> hasNot(final String key, final Object value) {
        super.hasNot(key, value);
        return this;
    }

    public AnnotatedListQueryBuilder<V> hasNot(final String key) {
        super.hasNot(key);
        return this;
    }

    public AnnotatedListQueryBuilder<V> has(final String key) {
        super.has(key);
        return this;
    }

    public AnnotatedListQueryBuilder<V> has(final String key, final BiPredicate biPredicate, final Object value) {
        super.has(key, biPredicate, value);
        return this;
    }

    public <T extends Comparable<?>> AnnotatedListQueryBuilder<V> interval(final String key, final T startValue, final T endValue) {
        super.interval(key, startValue, endValue);
        return this;
    }

    public AnnotatedListQueryBuilder<V> limit(final int limit) {
        super.limit(limit);
        return this;
    }

    public Iterable<AnnotatedValue<V>> annotatedValues() {
        throw new UnsupportedOperationException();
    }

    public Iterable<V> values() {
        throw new UnsupportedOperationException();
    }

    public AnnotatedListQuery build(final AnnotatedList annotatedList) {
        final AnnotatedListQuery query = annotatedList.query();
        for (final HasContainer hasContainer : this.hasContainers) {
            query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return query.limit(this.limit);
    }

    public AnnotatedListQueryBuilder build() {
        final AnnotatedListQueryBuilder builder = new AnnotatedListQueryBuilder();
        for (final HasContainer hasContainer : this.hasContainers) {
            builder.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return builder.limit(this.limit);
    }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        if (this.hasContainers.size() > 0)
            if (this.hasContainers.size() == 1)
                builder.append(this.hasContainers.get(0));
            else
                builder.append(this.hasContainers);
        if (this.limit != Integer.MAX_VALUE)
            builder.append(",").append(this.limit);
        return builder.toString();
    }
}
