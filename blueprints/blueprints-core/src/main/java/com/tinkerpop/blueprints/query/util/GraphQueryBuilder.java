package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryBuilder extends DefaultGraphQuery implements QueryBuilder {

    public GraphQuery ids(final Object... ids) {
        super.ids(ids);
        return this;
    }

    public GraphQueryBuilder has(final String key) {
        super.has(key);
        return this;
    }

    public GraphQueryBuilder hasNot(final String key) {
        super.hasNot(key);
        return this;
    }

    public GraphQueryBuilder has(final String key, final Object value) {
        super.has(key, value);
        return this;
    }

    public GraphQueryBuilder hasNot(final String key, final Object value) {
        super.hasNot(key, value);
        return this;
    }

    public GraphQueryBuilder has(final String key, final BiPredicate compare, final Object value) {
        super.has(key, compare, value);
        return this;
    }

    public <T extends Comparable<?>> GraphQueryBuilder interval(final String key, final T startValue, final T endValue) {
        super.interval(key, startValue, endValue);
        return this;
    }

    public Iterable<Edge> edges() {
        throw new UnsupportedOperationException();
    }

    public Iterable<Vertex> vertices() {
        throw new UnsupportedOperationException();
    }

    public long count() {
        throw new UnsupportedOperationException();
    }

    public long fingerPrint() {
        long id = 0l;
        this.hasContainers.stream().map(h -> Long.valueOf(h.key.hashCode() + h.predicate.hashCode() + h.value.hashCode())).reduce(id, (a, b) -> a + b);
        return id;
    }
}
