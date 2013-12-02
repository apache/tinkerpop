package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;

import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryBuilder extends DefaultVertexQuery implements QueryBuilder {

    public VertexQueryBuilder has(final String key) {
        super.has(key);
        return this;
    }

    public VertexQueryBuilder hasNot(final String key) {
        super.hasNot(key);
        return this;
    }

    public VertexQueryBuilder has(final String key, final Object value) {
        super.has(key, value);
        return this;
    }

    public VertexQueryBuilder hasNot(final String key, final Object value) {
        super.hasNot(key, value);
        return this;
    }

    public VertexQueryBuilder has(final String key, final BiPredicate compare, final Object value) {
        super.has(key, compare, value);
        return this;
    }

    public <T extends Comparable<?>> VertexQueryBuilder interval(final String key, final T startValue, final T endValue) {
        super.interval(key, startValue, endValue);
        return this;
    }

    public VertexQueryBuilder direction(final Direction direction) {
        super.direction(direction);
        return this;
    }

    public VertexQueryBuilder labels(final String... labels) {
        super.labels(labels);
        return this;
    }

    public VertexQueryBuilder limit(final int limit) {
        super.limit(limit);
        return this;
    }

    public VertexQueryBuilder reverse() {
        this.direction = this.direction.opposite();
        return this;
    }


    public VertexQuery build(final Vertex vertex) {
        VertexQuery query = vertex.query();
        for (final HasContainer hasContainer : this.hasContainers) {
            query = query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return query.limit(this.limit).labels(this.labels).direction(this.direction);
    }

    public VertexQueryBuilder build() {
        final VertexQueryBuilder query = new VertexQueryBuilder();
        query.direction(this.direction);
        query.labels(this.labels);
        for (final HasContainer hasContainer : this.hasContainers) {
            query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return query;
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
