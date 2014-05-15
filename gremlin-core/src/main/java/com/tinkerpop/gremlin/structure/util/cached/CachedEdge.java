package com.tinkerpop.gremlin.structure.util.cached;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class CachedEdge extends CachedElement implements Edge {
    private CachedVertex outVertex;
    private CachedVertex inVertex;

    public CachedEdge(final Object id, final String label, final Map<String,Object> properties,
                      final Pair<Object, String> outV, final Pair<Object, String> inV) {
        super(id, label, properties);
        this.outVertex = new CachedVertex(outV.getValue0(), outV.getValue1());
        this.inVertex = new CachedVertex(inV.getValue0(), inV.getValue1());
    }

    public CachedEdge(final Edge edge) {
        super(edge);
        final Vertex ov = edge.getVertex(Direction.OUT);
        final Vertex iv = edge.getVertex(Direction.IN);
        this.outVertex = new CachedVertex(ov.id(), ov.label());
        this.inVertex = new CachedVertex(iv.id(), iv.label());
    }

    public Vertex getVertex(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return outVertex;
        else if (direction.equals(Direction.IN))
            return inVertex;
        else
            throw Edge.Exceptions.bothIsNotSupported();
    }

    public String toString() {
        return StringFactory.edgeString(this);
    }
}
