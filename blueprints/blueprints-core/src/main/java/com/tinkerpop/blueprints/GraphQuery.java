package com.tinkerpop.blueprints;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface GraphQuery extends Query {

    public GraphQuery ids(Object... ids);

    @Override
    public GraphQuery has(String key);

    @Override
    public GraphQuery hasNot(String key);

    @Override
    public GraphQuery has(String key, BiPredicate biPredicate, Object value);

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue);

    @Override
    public GraphQuery limit(int limit);
}
