package com.tinkerpop.blueprints;

import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphQuery extends Query {

    @Override
    public GraphQuery has(String key);

    @Override
    public GraphQuery hasNot(String key);

    @Override
    public GraphQuery has(String key, Object value);

    @Override
    public GraphQuery hasNot(String key, Object value);

    @Override
    public GraphQuery has(String key, Predicate predicate, Object value);

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue);

    @Override
    public GraphQuery limit(int limit);
}
