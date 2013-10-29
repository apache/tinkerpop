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

    public Query has(String key);

    public Query hasNot(String key);

    public Query has(String key, BiPredicate biPredicate, Object value);

    public <T extends Comparable<?>> Query interval(String key, T startValue, T endValue);

    public Query limit(int limit);

    public Iterable<Edge> edges();

    public Iterable<Vertex> vertices();
}
