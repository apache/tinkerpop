package com.tinkerpop.blueprints.query;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.function.BiPredicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Luca Garulli (http://www.orientechnologies.com)
 * @author Daniel Kuppitz (daniel.kuppitz@shoproach.com)
 */
public interface GraphQuery extends Query {

    public GraphQuery ids(final Object... ids);

    @Override
    public GraphQuery has(final String key);

    @Override
    public GraphQuery hasNot(final String key);

    @Override
    public GraphQuery has(final String key, final BiPredicate biPredicate, final Object value);

    @Override
    public <T extends Comparable<?>> GraphQuery interval(final String key, final T startValue, final T endValue);

    @Override
    public GraphQuery limit(final int limit);

    public Iterable<Edge> edges();

    public Iterable<Vertex> vertices();

    // Defaults

    @Override
    public default GraphQuery has(final String key, final Object value) {
        return this.has(key, Compare.EQUAL, value);
    }
}
