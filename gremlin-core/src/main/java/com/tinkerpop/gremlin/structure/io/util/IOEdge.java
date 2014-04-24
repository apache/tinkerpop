package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IOEdge extends IOElement {
    public Object inV;
    public Object outV;

    public static IOEdge from(final Edge edge) {
        final IOEdge ioe = new IOEdge();
        ioe.inV = edge.getVertex(Direction.IN).getId();
        ioe.outV = edge.getVertex(Direction.OUT).getId();
        return from(edge, ioe);
    }
}
