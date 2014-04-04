package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.Comparators;
import com.tinkerpop.gremlin.util.function.FunctionUtils;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONModule extends SimpleModule {

    public static final String TOKEN_ID = "id";
    public static final String TOKEN_TYPE = "type";
    public static final String TOKEN_VALUE = "value";
    public static final String TOKEN_PROPERTIES = "properties";
    public static final String TOKEN_EDGE = "edge";
    public static final String TOKEN_EDGES = "edges";
    public static final String TOKEN_VERTEX = "vertex";
    public static final String TOKEN_VERTICES = "vertices";
    public static final String TOKEN_IN = "in";
    public static final String TOKEN_OUT = "out";
    public static final String TOKEN_LABEL = "label";

    public GraphSONModule(final boolean normalize) {
        super("graphson");
        addSerializer(Edge.class, new EdgeJacksonSerializer(normalize));
        addSerializer(Property.class, new PropertyJacksonSerializer());
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
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_ID, edge.getId());
            jsonGenerator.writeStringField(TOKEN_LABEL, edge.getLabel());
            jsonGenerator.writeStringField(TOKEN_TYPE, TOKEN_EDGE);
            jsonGenerator.writeObjectField(TOKEN_IN, edge.getVertex(Direction.IN).getId());
            jsonGenerator.writeObjectField(TOKEN_OUT, edge.getVertex(Direction.OUT).getId());

            if (normalize){
                jsonGenerator.writeObjectFieldStart(GraphSONModule.TOKEN_PROPERTIES);
                edge.getProperties().entrySet().stream().sorted(Comparators.PROPERTY_ENTRY_COMPARATOR)
                        .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.getValue())));
                jsonGenerator.writeEndObject();
            } else
                jsonGenerator.writeObjectField(TOKEN_PROPERTIES, edge.getProperties());


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
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_ID, vertex.getId());
            jsonGenerator.writeStringField(TOKEN_LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(TOKEN_TYPE, TOKEN_VERTEX);

            if (normalize) {
                jsonGenerator.writeObjectFieldStart(GraphSONModule.TOKEN_PROPERTIES);
                vertex.getProperties().entrySet().stream().sorted(Comparators.PROPERTY_ENTRY_COMPARATOR)
                        .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.getValue())));
                jsonGenerator.writeEndObject();
            } else
                jsonGenerator.writeObjectField(TOKEN_PROPERTIES, vertex.getProperties());

            jsonGenerator.writeEndObject();
        }
    }

    // todo: should properties be encoded as Map, or just stream out values as Property...would remove hierarchy

    static class PropertyJacksonSerializer extends StdSerializer<Property> {
        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(TOKEN_VALUE, property.get());
            jsonGenerator.writeEndObject();
        }
    }

    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    static class GraphSONKeySerializer extends StdKeySerializer {
        @Override
        public void serialize(Object o, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            if (Element.class.isAssignableFrom(o.getClass()))
                jsonGenerator.writeFieldName((((Element) o).getId()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }
}
