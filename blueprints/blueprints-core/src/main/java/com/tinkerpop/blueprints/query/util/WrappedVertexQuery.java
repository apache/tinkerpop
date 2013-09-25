package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class WrappedVertexQuery implements VertexQuery {

    protected VertexQuery query;

    public WrappedVertexQuery(final VertexQuery query) {
        this.query = query;
    }

    public VertexQuery has(final String key) {
        this.query = this.query.has(key);
        return this;
    }

    public VertexQuery hasNot(final String key) {
        this.query = this.query.hasNot(key);
        return this;
    }

    public VertexQuery has(final String key, final BiPredicate compare, final Object value) {
        this.query = this.query.has(key, compare, value);
        return this;
    }

    public <T extends Comparable<?>> VertexQuery interval(final String key, final T startValue, final T endValue) {
        this.query = this.query.interval(key, startValue, endValue);
        return this;
    }

    public VertexQuery direction(final Direction direction) {
        this.query = this.query.direction(direction);
        return this;
    }

    public VertexQuery limit(final int limit) {
        this.query = this.query.limit(limit);
        return this;
    }

    public VertexQuery labels(final String... labels) {
        this.query = this.query.labels(labels);
        return this;
    }

    public long count() {
        return this.query.count();
    }

    public abstract Iterable<Edge> edges();

    public abstract Iterable<Vertex> vertices();

}
