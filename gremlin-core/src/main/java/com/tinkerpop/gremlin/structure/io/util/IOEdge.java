package com.tinkerpop.gremlin.structure.io.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedEdge;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;
import org.javatuples.Pair;

/**
 * Serializable form of {@link Edge} for IO purposes.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IOEdge extends IOElement {
    public Object inV;
    public Object outV;
    public String inVLabel;
    public String outVLabel;

    public static IOEdge from(final Edge edge) {
        final IOEdge ioe = new IOEdge();
        final Vertex in = edge.getVertex(Direction.IN);
        final Vertex out = edge.getVertex(Direction.OUT);
        ioe.inV = in.getId();
        ioe.outV = out.getId();
        ioe.inVLabel = in.getLabel();
        ioe.outVLabel = out.getLabel();
        return from(edge, ioe);
    }
}
