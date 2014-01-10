package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.VertexQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * For those graph engines that do not support the low-level querying of the edges of a vertex, then
 * {@link DefaultVertexQuery} can be used. It assumes, at minimum, that
 * {@link com.tinkerpop.blueprints.query.VertexQuery#vertices()} is implemented by the respective {@link Vertex}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultVertexQuery extends DefaultQuery implements VertexQuery {

    private static final String[] EMPTY_LABELS = new String[]{};
    public Direction direction = Direction.BOTH;
    public String[] labels = EMPTY_LABELS;
    public List<Vertex> adjacents = new ArrayList<>();

    public VertexQuery adjacents(final Vertex... vertices) {
        this.adjacents = Arrays.asList(vertices);
        return this;
    }

    public VertexQuery has(final String key) {
        super.has(key);
        return this;
    }

    public VertexQuery hasNot(final String key) {
        super.hasNot(key);
        return this;
    }

    public VertexQuery has(final String key, final Object value) {
        super.has(key, value);
        return this;
    }

    public VertexQuery hasNot(final String key, final Object value) {
        super.hasNot(key, value);
        return this;
    }

    public VertexQuery has(final String key, final BiPredicate biPredicate, final Object value) {
        super.has(key, biPredicate, value);
        return this;
    }

    public <T extends Comparable<?>> VertexQuery interval(final String key, final T startValue, final T endValue) {
        super.interval(key, startValue, endValue);
        return this;
    }

    public VertexQuery limit(final int limit) {
        super.limit(limit);
        return this;
    }

    public VertexQuery direction(final Direction direction) {
        this.direction = direction;
        return this;
    }

    public VertexQuery labels(final String... labels) {
        this.labels = labels;
        return this;
    }

    public abstract Iterable<Edge> edges();

    public abstract Iterable<Vertex> vertices();

    public abstract long count();
}
