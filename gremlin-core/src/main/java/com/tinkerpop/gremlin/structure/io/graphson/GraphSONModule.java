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
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONTokens.ID, edge.getId());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.getLabel());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.EDGE);
            jsonGenerator.writeObjectField(GraphSONTokens.IN, edge.getVertex(Direction.IN).getId());
            jsonGenerator.writeObjectField(GraphSONTokens.OUT, edge.getVertex(Direction.OUT).getId());

            if (normalize){
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
                edge.getProperties().values().stream().sorted(Comparators.PROPERTY_COMPARATOR)
                        .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.get())));
                jsonGenerator.writeEndObject();
            } else {
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
                edge.getProperties().values().forEach(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.get())));
                jsonGenerator.writeEndObject();
            }


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
            jsonGenerator.writeObjectField(GraphSONTokens.ID, vertex.getId());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            if (normalize) {
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
                vertex.getProperties().values().stream().sorted(Comparators.PROPERTY_COMPARATOR)
                        .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.get())));
                jsonGenerator.writeEndObject();
            } else {
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
                vertex.getProperties().values().forEach(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.get())));
                jsonGenerator.writeEndObject();
            }

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
