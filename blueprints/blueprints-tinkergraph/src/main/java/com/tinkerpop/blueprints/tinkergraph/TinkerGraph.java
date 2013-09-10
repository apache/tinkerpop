package com.tinkerpop.blueprints.tinkergraph;

import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.OLTPGraph;
import com.tinkerpop.blueprints.Property;
import com.tinkerpop.blueprints.Vertex;

import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TinkerGraph implements OLTPGraph {

    private Map<String, Vertex> vertices;

    public Vertex addVertex(final Property... properties) {
        return null;
    }

    public GraphQuery query() {
        return null;
    }

    public void commit() {
        throw new UnsupportedOperationException();
    }

    public void rollback() {
        throw new UnsupportedOperationException();
    }

    public Features getFeatures() {
        return null;
    }

}
