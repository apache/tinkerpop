package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.io.util.IOAnnotatedList;
import com.tinkerpop.gremlin.structure.util.Comparators;
import com.tinkerpop.gremlin.util.function.FunctionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONModule extends SimpleModule {

    public GraphSONModule(final boolean normalize) {
        super("graphson");
        addSerializer(Edge.class, new EdgeJacksonSerializer(normalize));
        addSerializer(Vertex.class, new VertexJacksonSerializer(normalize));
        addSerializer(GraphSONVertex.class, new GraphSONVertex.VertexJacksonSerializer(normalize));
        addSerializer(GraphSONGraph.class, new GraphSONGraph.GraphJacksonSerializer(normalize));
    }

    static class EdgeJacksonSerializer extends StdSerializer<Edge> {
        private final boolean normalize;
        public EdgeJacksonSerializer(final boolean normalize) {
            super(Edge.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(edge, jsonGenerator, false);
        }

        @Override
        public void serializeWithType(final Edge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            ser(edge, jsonGenerator, true);
        }

        private void ser(final Edge edge, final JsonGenerator jsonGenerator, final boolean includeType) throws IOException, JsonProcessingException {
            jsonGenerator.writeStartObject();
            if (includeType)
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, GraphSONTokens.MAP_CLASS);

            jsonGenerator.writeObjectField(GraphSONTokens.ID, edge.getId());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.getLabel());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.EDGE);
            jsonGenerator.writeObjectField(GraphSONTokens.IN, edge.getVertex(Direction.IN).getId());
            jsonGenerator.writeObjectField(GraphSONTokens.OUT, edge.getVertex(Direction.OUT).getId());

            jsonGenerator.writeObjectField(GraphSONTokens.PROPERTIES,
                        edge.getProperties().values().stream().collect(
                                Collectors.toMap(Property::getKey, Property::get)));

            jsonGenerator.writeEndObject();
        }
    }

    static class VertexJacksonSerializer extends StdSerializer<Vertex> {
        final boolean normalize;
        public VertexJacksonSerializer(final boolean normalize) {
            super(Vertex.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(vertex, jsonGenerator, false);
        }

        @Override
        public void serializeWithType(final Vertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            ser(vertex, jsonGenerator, true);

        }

        private void ser(final Vertex vertex, final JsonGenerator jsonGenerator, final boolean includeType)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            if (includeType)
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, GraphSONTokens.MAP_CLASS);

            jsonGenerator.writeObjectField(GraphSONTokens.ID, vertex.getId());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            jsonGenerator.writeObjectField(GraphSONTokens.PROPERTIES,
                vertex.getProperties().values().stream().collect(
                    Collectors.toMap(Property::getKey, p -> (p.get() instanceof AnnotatedList) ? IOAnnotatedList.from((AnnotatedList) p.get()) : p.get())));

            jsonGenerator.writeEndObject();
        }

    }

    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    static class GraphSONKeySerializer extends StdKeySerializer {
        @Override
        public void serialize(final Object o, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            ser(o, jsonGenerator, serializerProvider);
        }

        @Override
        public void serializeWithType(final Object o, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            ser(o, jsonGenerator, serializerProvider);
        }

        private void ser(final Object o, final JsonGenerator jsonGenerator,
                         final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            if (Element.class.isAssignableFrom(o.getClass()))
                jsonGenerator.writeFieldName((((Element) o).getId()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }
}
