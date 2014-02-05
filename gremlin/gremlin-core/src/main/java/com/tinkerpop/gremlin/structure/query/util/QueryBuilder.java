package com.tinkerpop.gremlin.structure.query.util;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.query.Query;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface QueryBuilder extends Query {

    public default Iterable<Edge> edges() {
        throw new UnsupportedOperationException();
    }

    public default Iterable<Vertex> vertices() {
        throw new UnsupportedOperationException();
    }

    public default long count() {
        throw new UnsupportedOperationException();
    }
}
