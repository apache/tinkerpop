package com.tinkerpop.blueprints;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface VertexQuery extends Query {

    public VertexQuery direction(Direction direction);

    public VertexQuery labels(String... labels);

    @Override
    public VertexQuery has(String key);

    @Override
    public VertexQuery hasNot(String key);

    @Override
    public VertexQuery has(String key, BiPredicate biPredicate, Object value);

    @Override
    public <T extends Comparable<?>> VertexQuery interval(String key, T startValue, T endValue);

    @Override
    public VertexQuery limit(int limit);

    public long count();

}
