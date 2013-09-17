package com.tinkerpop.blueprints;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Query {

    public Query has(String key);

    public Query hasNot(String key);

    public Query has(String key, BiPredicate biPredicate, Object value);

    public <T extends Comparable<?>> Query interval(String key, T startValue, T endValue);

    public Query limit(int limit);

    public Iterable<Edge> edges();

    public Iterable<Vertex> vertices();
}
