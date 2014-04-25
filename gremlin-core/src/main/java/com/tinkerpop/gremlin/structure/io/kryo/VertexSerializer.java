package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IOVertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;

/**
 * Serializes any Vertex implementation encountered to an {@link IOVertex} and deserializes it out to a
 * {@link CachedVertex}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class VertexSerializer extends Serializer<Vertex> {
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