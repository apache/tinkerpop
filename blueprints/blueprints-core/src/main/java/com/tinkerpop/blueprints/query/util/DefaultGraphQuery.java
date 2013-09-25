package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.GraphQuery;

import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * For those graph engines that do not support the low-level querying of the vertices or edges, then DefaultGraphQuery can be used.
 * DefaultGraphQuery assumes, at minimum, that Graph.getVertices() and Graph.getEdges() is implemented by the respective Graph.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DefaultGraphQuery extends DefaultQuery implements GraphQuery {

    public GraphQuery ids(final Object... ids) {
        Stream.of(ids).forEach(id -> {
            this.hasContainers.add(new HasContainer(Property.Key.ID.toString(), Compare.EQUAL, id));
        });
        return this;
    }

    public GraphQuery has(final String key) {
        super.has(key);
        return this;
    }

    public GraphQuery hasNot(final String key) {
        super.hasNot(key);
        return this;
    }

    public GraphQuery has(final String key, final Object value) {
        super.has(key, value);
        return this;
    }

    public GraphQuery hasNot(final String key, final Object value) {
        super.hasNot(key, value);
        return this;
    }

    public GraphQuery has(final String key, final BiPredicate biPredicate, final Object value) {
        super.has(key, biPredicate, value);
        return this;
    }

    public <T extends Comparable<?>> GraphQuery interval(final String key, final T startValue, final T endValue) {
        super.interval(key, startValue, endValue);
        return this;
    }

    public GraphQuery limit(final int limit) {
        super.limit(limit);
        return this;
    }

    public abstract Iterable<Edge> edges();

    public abstract Iterable<Vertex> vertices();

}
