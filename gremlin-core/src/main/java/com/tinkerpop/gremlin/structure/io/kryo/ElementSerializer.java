package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IOEdge;
import com.tinkerpop.gremlin.structure.io.util.IOVertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedEdge;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;
import org.javatuples.Pair;

/**
 * Holder class for {@link com.tinkerpop.gremlin.structure.Element} serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ElementSerializer {
    /**
     * Serializes any Vertex implementation encountered to an {@link com.tinkerpop.gremlin.structure.io.util.IOEdge} and deserializes it out to a
     * {@link com.tinkerpop.gremlin.structure.util.cached.CachedEdge}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class EdgeSerializer extends Serializer<Edge> {
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

    /**
     * Serializes any Vertex implementation encountered to an {@link com.tinkerpop.gremlin.structure.io.util.IOVertex} and deserializes it out to a
     * {@link com.tinkerpop.gremlin.structure.util.cached.CachedVertex}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class VertexSerializer extends Serializer<Vertex> {
        public VertexSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Vertex vertex) {
            kryo.writeClassAndObject(output, IOVertex.from(vertex));
        }

        @Override
        public Vertex read(final Kryo kryo, final Input input, final Class<Vertex> vertexClass) {
            final IOVertex iov = (IOVertex) kryo.readClassAndObject(input);
            return new CachedVertex(iov.id, iov.label, iov.properties);
        }
    }
}
