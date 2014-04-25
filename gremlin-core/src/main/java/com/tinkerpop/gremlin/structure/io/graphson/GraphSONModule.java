package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedList;
import com.tinkerpop.gremlin.structure.util.cached.CachedEdge;
import com.tinkerpop.gremlin.structure.util.cached.CachedVertex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

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

        // todo: serialize as IOVertex/Edge and deserialize to CachedVertex when types are embedded..add tests
        addDeserializer(Edge.class, new EdgeJacksonDeserializer());
        addDeserializer(Vertex.class, new VertexJacksonDeserializer());
    }

    static class EdgeJacksonDeserializer extends StdDeserializer<Edge> {

        public EdgeJacksonDeserializer() {
            super(Edge.class);
        }

        @Override
        public Edge deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
            final ObjectNode root = mapper.readTree(jsonParser);

            return new CachedEdge("1", "test", null, null, null);

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
            final Map<String,Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, edge.getId());
            m.put(GraphSONTokens.LABEL, edge.getLabel());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.EDGE);
            m.put(GraphSONTokens.IN, edge.getVertex(Direction.IN).getId());
            m.put(GraphSONTokens.OUT, edge.getVertex(Direction.OUT).getId());
            m.put(GraphSONTokens.PROPERTIES,
                    edge.getProperties().values().stream().collect(
                            Collectors.toMap(Property::getKey, Property::get)));

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
            final Map<String,Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.getId());
            m.put(GraphSONTokens.LABEL, vertex.getLabel());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);
            m.put(GraphSONTokens.PROPERTIES,
                    vertex.getProperties().values().stream().collect(
                            Collectors.toMap(Property::getKey, p -> (p.get() instanceof AnnotatedList) ? IOAnnotatedList.from((AnnotatedList) p.get()) : p.get())));

            jsonGenerator.writeObject(m);
        }

    }

    static class VertexJacksonDeserializer extends StdDeserializer<Vertex> {

        public VertexJacksonDeserializer() {
            super(Vertex.class);
        }

        @Override
        public Vertex deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final ObjectMapper mapper = (ObjectMapper) jsonParser.getCodec();
            final ObjectNode root = mapper.readTree(jsonParser);

            return new CachedVertex("1", "test");

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
                jsonGenerator.writeFieldName((((Element) o).getId()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }
}
