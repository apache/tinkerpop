package com.tinkerpop.gremlin.structure.query.util;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.VertexQuery;

import java.util.Arrays;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryBuilder extends DefaultVertexQuery implements QueryBuilder {

    public VertexQueryBuilder adjacents(final Vertex... vertices) {
        super.adjacents(vertices);
        return this;
    }

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
      //  final VertexQuery query = vertex.query();
      //  for (final HasContainer hasContainer : this.hasContainers) {
      //      query.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
      //  }
      // return query.limit(this.limit).labels(this.labels).direction(this.direction);
        return null;
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

    public String toString() {
        final StringBuilder builder = new StringBuilder(this.direction.toString());
        if (this.labels.length > 0)
            if (this.labels.length == 1)
                builder.append(",").append(this.labels[0]);
            else
                builder.append(",").append(Arrays.asList(this.labels));
        if (this.hasContainers.size() > 0)
            if (this.hasContainers.size() == 1)
                builder.append(",").append(this.hasContainers.get(0));
            else
                builder.append(",").append(this.hasContainers);
        if (this.limit != Integer.MAX_VALUE)
            builder.append(",").append(this.limit);
        return builder.toString();
    }
}
