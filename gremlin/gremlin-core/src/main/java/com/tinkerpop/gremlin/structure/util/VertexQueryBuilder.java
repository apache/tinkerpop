package com.tinkerpop.gremlin.structure.util;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Contains;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class VertexQueryBuilder {

    private static final String[] EMPTY_LABELS = new String[]{};
    public Direction direction = Direction.BOTH;
    public String[] labels = EMPTY_LABELS;
    public int limit = Integer.MAX_VALUE;
    public List<HasContainer> hasContainers = new ArrayList<>();

    public VertexQueryBuilder has(final String key) {
        this.hasContainers.add(new HasContainer(key, Contains.IN));
        return this;
    }

    public VertexQueryBuilder hasNot(final String key) {
        this.hasContainers.add(new HasContainer(key, Contains.NOT_IN));
        return this;
    }

    public VertexQueryBuilder has(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.EQUAL, value));
        return this;
    }

    public VertexQueryBuilder hasNot(final String key, final Object value) {
        this.hasContainers.add(new HasContainer(key, Compare.NOT_EQUAL, value));
        return this;
    }

    public VertexQueryBuilder has(final String key, final BiPredicate compare, final Object value) {
        this.hasContainers.add(new HasContainer(key, compare, value));
        return this;
    }

    public <T extends Comparable<?>> VertexQueryBuilder interval(final String key, final T startValue, final T endValue) {
        this.hasContainers.add(new HasContainer(key, Compare.GREATER_THAN_EQUAL, startValue));
        this.hasContainers.add(new HasContainer(key, Compare.LESS_THAN, endValue));
        return this;
    }

    public VertexQueryBuilder direction(final Direction direction) {
        this.direction = direction;
        return this;
    }

    public VertexQueryBuilder labels(final String... labels) {
        this.labels = labels;
        return this;
    }

    public VertexQueryBuilder reverse() {
        this.direction = this.direction.opposite();
        return this;
    }

    public GraphTraversal<Vertex, Edge> build(final Vertex vertex) {
        GraphTraversal traversal = vertex.identity();
        if (this.direction.equals(Direction.OUT))
            traversal.outE(this.limit, this.labels);
        else if (this.direction.equals(Direction.IN))
            traversal.inE(this.limit, this.labels);
        else if (this.direction.equals(Direction.BOTH))
            traversal.bothE(this.limit, this.labels);
        for (final HasContainer hasContainer : this.hasContainers) {
            traversal.has(hasContainer.key, hasContainer.predicate, hasContainer.value);
        }
        return traversal;
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
