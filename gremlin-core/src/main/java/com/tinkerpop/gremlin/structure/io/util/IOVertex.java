package com.tinkerpop.gremlin.structure.io.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IOVertex extends IOElement {
    public static IOVertex from(final Vertex vertex) {
        final IOVertex iov = new IOVertex();
        return from(vertex, iov);
    }

    public static class VertexSerializer extends Serializer<Vertex> {
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
