package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONModule extends SimpleModule {

    public GraphSONModule(final boolean normalize) {
        super("graphson");
        addSerializer(Edge.class, new EdgeJacksonSerializer());
        addSerializer(Vertex.class, new VertexJacksonSerializer());
        addSerializer(GraphSONVertex.class, new GraphSONVertex.VertexJacksonSerializer());
        addSerializer(GraphSONGraph.class, new GraphSONGraph.GraphJacksonSerializer(normalize));
        addSerializer(GraphSONVertexProperty.class, new GraphSONVertexProperty.GraphSONVertexPropertySerializer());
        addSerializer(VertexProperty.class, new VertexPropertyJacksonSerializer());
        addSerializer(Property.class, new PropertyJacksonSerializer());
    }

    static class VertexPropertyJacksonSerializer extends StdSerializer<VertexProperty> {
        public VertexPropertyJacksonSerializer() {
            super(VertexProperty.class);
        }

        @Override
        public void serialize(final VertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final VertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator);
        }

        private void ser(final VertexProperty property, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, property.id());
            m.put(GraphSONTokens.VALUE, property.value());
            m.put(GraphSONTokens.LABEL, property.label());
            m.put(GraphSONTokens.PROPERTIES, props(property));

            jsonGenerator.writeObject(m);
        }

        private Map<String, Object> props(final VertexProperty property) {
            if (property instanceof DetachedVertexProperty) {
                try {
                    return IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value);
                } catch (UnsupportedOperationException uoe) {
                    return new HashMap<>();
                }
            } else {
                return (property.graph().features().vertex().supportsMetaProperties()) ?
                        IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value) :
                        new HashMap<>();
            }
        }
    }

    static class PropertyJacksonSerializer extends StdSerializer<Property> {
        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Property property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator);
        }

        private void ser(final Property property, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.VALUE, property.value());
            jsonGenerator.writeObject(m);
        }
    }


    static class EdgeJacksonSerializer extends StdSerializer<Edge> {
        public EdgeJacksonSerializer() {
            super(Edge.class);
        }

        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(edge, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Edge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(edge, jsonGenerator);
        }

        private void ser(final Edge edge, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, edge.id());
            m.put(GraphSONTokens.LABEL, edge.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.EDGE);

            final Map<String, Object> properties = IteratorUtils.collectMap(edge.iterators().propertyIterator(), Property::key, Property::value);
            m.put(GraphSONTokens.PROPERTIES, properties);

            final Vertex inV = edge.iterators().vertexIterator(Direction.IN).next();
            m.put(GraphSONTokens.IN, inV.id());
            m.put(GraphSONTokens.IN_LABEL, inV.label());

            final Vertex outV = edge.iterators().vertexIterator(Direction.OUT).next();
            m.put(GraphSONTokens.OUT, outV.id());
            m.put(GraphSONTokens.OUT_LABEL, outV.label());

            jsonGenerator.writeObject(m);
        }
    }

    static class VertexJacksonSerializer extends StdSerializer<Vertex> {

        public VertexJacksonSerializer() {
            super(Vertex.class);
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(vertex, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Vertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(vertex, jsonGenerator);

        }

        private void ser(final Vertex vertex, final JsonGenerator jsonGenerator)
                throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.id());
            m.put(GraphSONTokens.LABEL, vertex.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            // convert to GraphSONVertexProperty so that the label does not get serialized in the output - it is
            // redundant because the key in the map is the same as the label.
            final Iterator<GraphSONVertexProperty> vertexPropertyList = IteratorUtils.map(vertex.iterators().propertyIterator(), GraphSONVertexProperty::new);
            final Object properties = IteratorUtils.groupBy(vertexPropertyList, vp -> vp.getToSerialize().key());
            m.put(GraphSONTokens.PROPERTIES, properties);

            jsonGenerator.writeObject(m);
        }

    }

    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    static class GraphSONKeySerializer extends StdKeySerializer {
        @Override
        public void serialize(final Object o, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
            ser(o, jsonGenerator, serializerProvider);
        }

        @Override
        public void serializeWithType(final Object o, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(o, jsonGenerator, serializerProvider);
        }

        private void ser(final Object o, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider) throws IOException {
            if (Element.class.isAssignableFrom(o.getClass()))
                jsonGenerator.writeFieldName((((Element) o).id()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }
}
