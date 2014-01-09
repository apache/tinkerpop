package com.tinkerpop.blueprints.query;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.util.function.BiPredicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Luca Garulli (http://www.orientechnologies.com)
 * @author Daniel Kuppitz (daniel.kuppitz@shoproach.com)
 */
public interface Query {

    public Query has(final String key);

    public Query hasNot(final String key);

    public Query has(final String key, final BiPredicate biPredicate, final Object value);

    public <T extends Comparable<?>> Query interval(final String key, final T startValue, final T endValue);

    public Query limit(final int limit);

}
