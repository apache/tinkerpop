package com.tinkerpop.blueprints.query;

import java.util.function.BiPredicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Luca Garulli (http://www.orientechnologies.com)
 * @author Daniel Kuppitz (daniel.kuppitz@shoproach.com)
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
