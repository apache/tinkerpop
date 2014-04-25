package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.io.util.IOEdge;
import com.tinkerpop.gremlin.structure.util.cached.CachedEdge;
import org.javatuples.Pair;

/**
 * Serializes any Vertex implementation encountered to an {@link IOEdge} and deserializes it out to a
 * {@link CachedEdge}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class EdgeSerializer extends Serializer<Edge> {
    @Override
    public void write(final Kryo kryo, final Output output, final Edge edge) {
        kryo.writeClassAndObject(output, IOEdge.from(edge));
    }

    @Override
    public Edge read(final Kryo kryo, final Input input, final Class<Edge> edgeClass) {
        final IOEdge ioe = (IOEdge) kryo.readClassAndObject(input);
        return new CachedEdge(ioe.id, ioe.label, ioe.properties,
                Pair.with(ioe.outV, ioe.outVLabel), Pair.with(ioe.inV, ioe.inVLabel));
    }
}
