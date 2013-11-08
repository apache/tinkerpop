package com.tinkerpop.blueprints.query.util;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.query.Query;

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

    public long fingerPrint();
}
