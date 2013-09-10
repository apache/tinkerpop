package com.tinkerpop.blueprints;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Query {

    public Query has(String key);

    public Query hasNot(String key);

    public Query has(String key, Object value);

    public Query hasNot(String key, Object value);

    public Query has(String key, Predicate predicate, Object value);

    public <T extends Comparable<?>> Query interval(String key, T startValue, T endValue);

    public Query limit(int limit);

    public long count();

    public Iterable<Edge> edges();

    public Iterable<Vertex> vertices();
}
