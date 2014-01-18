package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class GraphQueryBuilder extends DefaultGraphQuery implements QueryBuilder {

    public GraphQueryBuilder ids(final Object... ids) {
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

    public GraphQuery build(final Graph graph) {
        final GraphQuery query = graph.query();
        for (final HasContainer hasContainer : this.hasContainers) {
            query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return query.limit(this.limit);
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
}
