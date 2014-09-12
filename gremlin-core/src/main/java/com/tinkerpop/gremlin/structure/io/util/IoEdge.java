package com.tinkerpop.gremlin.structure.io.util;

import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;

/**
 * Serializable form of {@link Edge} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IoEdge extends IoElement {
    public Object inV;
    public Object outV;
    public String inVLabel;
    public String outVLabel;

    public static IoEdge from(final Edge edge) {
        final IoEdge ioe = new IoEdge();
        final Vertex in = edge.iterators().vertices(Direction.IN).next();
        final Vertex out = edge.iterators().vertices(Direction.OUT).next();
        ioe.inV = in.id();
        ioe.outV = out.id();
        ioe.inVLabel = in.label();
        ioe.outVLabel = out.label();
        return from(edge, ioe);
    }
}
