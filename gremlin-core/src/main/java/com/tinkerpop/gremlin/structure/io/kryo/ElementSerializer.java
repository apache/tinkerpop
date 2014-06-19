package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IoEdge;
import com.tinkerpop.gremlin.structure.io.util.IoVertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.javatuples.Pair;

/**
 * Traverser class for {@link com.tinkerpop.gremlin.structure.Element} serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ElementSerializer {
    /**
     * Serializes any Vertex implementation encountered to an {@link com.tinkerpop.gremlin.structure.io.util.IoEdge} and deserializes it out to a
     * {@link com.tinkerpop.gremlin.structure.util.detached.DetachedEdge}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class EdgeSerializer extends Serializer<Edge> {
        @Override
        public void write(final Kryo kryo, final Output output, final Edge edge) {
            kryo.writeClassAndObject(output, IoEdge.from(edge));
        }

        @Override
        public Edge read(final Kryo kryo, final Input input, final Class<Edge> edgeClass) {
            final IoEdge ioe = (IoEdge) kryo.readClassAndObject(input);
            return new DetachedEdge(ioe.id, ioe.label, ioe.properties, ioe.hiddenProperties,
                    Pair.with(ioe.outV, ioe.outVLabel), Pair.with(ioe.inV, ioe.inVLabel));
        }
    }

    /**
     * Serializes any Vertex implementation encountered to an {@link com.tinkerpop.gremlin.structure.io.util.IoVertex} and deserializes it out to a
     * {@link com.tinkerpop.gremlin.structure.util.detached.DetachedVertex}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class VertexSerializer extends Serializer<Vertex> {
        public VertexSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Vertex vertex) {
            kryo.writeClassAndObject(output, IoVertex.from(vertex));
        }

        @Override
        public Vertex read(final Kryo kryo, final Input input, final Class<Vertex> vertexClass) {
            final IoVertex iov = (IoVertex) kryo.readClassAndObject(input);
            return new DetachedVertex(iov.id, iov.label, iov.properties, iov.hiddenProperties);
        }
    }
}
