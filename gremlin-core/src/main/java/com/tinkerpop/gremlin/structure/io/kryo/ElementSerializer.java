package com.tinkerpop.gremlin.structure.io.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import com.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

/**
 * Traverser class for {@link com.tinkerpop.gremlin.structure.Element} serializers.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ElementSerializer {
    /**
     * Serializes any Vertex implementation encountered to a {@link DetachedEdge}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class EdgeSerializer extends Serializer<Edge> {
        @Override
        public void write(final Kryo kryo, final Output output, final Edge edge) {
            kryo.writeClassAndObject(output, DetachedEdge.detach(edge));
        }

        @Override
        public Edge read(final Kryo kryo, final Input input, final Class<Edge> edgeClass) {
            final Object o = kryo.readClassAndObject(input);
            return (Edge) o;
        }
    }

    /**
     * Serializes any Vertex implementation encountered to an {@link DetachedVertex}.
     *
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    static class VertexSerializer extends Serializer<Vertex> {
        public VertexSerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Vertex vertex) {
            kryo.writeClassAndObject(output, vertex instanceof DetachedVertex ? (DetachedVertex) vertex : DetachedVertex.detach(vertex));
        }

        @Override
        public Vertex read(final Kryo kryo, final Input input, final Class<Vertex> vertexClass) {
            return (Vertex) kryo.readClassAndObject(input);
        }
    }

    static class PropertySerializer extends Serializer<Property> {
        public PropertySerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final Property property) {
            kryo.writeClassAndObject(output, property instanceof DetachedProperty ? (DetachedProperty) property : DetachedProperty.detach(property));
        }

        @Override
        public Property read(final Kryo kryo, final Input input, final Class<Property> propertyClass) {
            return (Property) kryo.readClassAndObject(input);
        }
    }

    static class VertexPropertySerializer extends Serializer<VertexProperty> {
        public VertexPropertySerializer() {
        }

        @Override
        public void write(final Kryo kryo, final Output output, final VertexProperty property) {
            kryo.writeClassAndObject(output, property instanceof DetachedVertexProperty ? (DetachedVertexProperty) property : DetachedVertexProperty.detach(property));
        }

        @Override
        public VertexProperty read(final Kryo kryo, final Input input, final Class<VertexProperty> propertyClass) {
            return (VertexProperty) kryo.readClassAndObject(input);
        }
    }
}
