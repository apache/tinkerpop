package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.AnnotatedList;
import com.tinkerpop.gremlin.structure.Direction;
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
class GraphSONVertex {
    private final Direction direction;
    private final Vertex vertexToSerialize;

    public GraphSONVertex(final Vertex vertexToSerialize, final Direction direction) {
        this.direction = direction;
        this.vertexToSerialize = vertexToSerialize;
    }

    public Direction getDirection() {
        return direction;
    }

    public Vertex getVertexToSerialize() {
        return vertexToSerialize;
    }

    static class VertexJacksonSerializer extends StdSerializer<GraphSONVertex> {
        private final boolean normalize;

        public VertexJacksonSerializer(final boolean normalize) {
            super(GraphSONVertex.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(directionalVertex, jsonGenerator, false);
        }

        @Override
        public void serializeWithType(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
            ser(directionalVertex, jsonGenerator, true);
        }

        public void ser(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator, final boolean includeType) throws IOException, JsonProcessingException {
            final Vertex vertex = directionalVertex.getVertexToSerialize();
            jsonGenerator.writeStartObject();
            if (includeType)
                jsonGenerator.writeStringField(GraphSONTokens.CLASS, GraphSONTokens.MAP_CLASS);

            jsonGenerator.writeObjectField(GraphSONTokens.ID, vertex.getId());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            if (normalize) {
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);

                // if required manually set the @class since a sorted map is required here and must be manually written
                if (includeType)
                    jsonGenerator.writeStringField(GraphSONTokens.CLASS, GraphSONTokens.MAP_CLASS);

                vertex.getProperties().values().stream().sorted(Comparators.PROPERTY_COMPARATOR)
                        .forEachOrdered(FunctionUtils.wrapConsumer(e -> jsonGenerator.writeObjectField(e.getKey(), e.get())));
                jsonGenerator.writeEndObject();
            } else {
                jsonGenerator.writeObjectField(GraphSONTokens.PROPERTIES,
                        vertex.getProperties().values().stream().collect(
                                Collectors.toMap(Property::getKey, p -> (p.get() instanceof AnnotatedList) ? IOAnnotatedList.from((AnnotatedList) p.get()) : p.get())));
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.OUT) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.OUT);
                if (normalize)
                    vertex.outE().order(Comparators.HELD_EDGE_COMPARATOR).forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
                else
                    vertex.outE().forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
                jsonGenerator.writeEndArray();
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.IN) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.IN);
                if (normalize)
                    vertex.inE().order(Comparators.HELD_EDGE_COMPARATOR).forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
                else
                    vertex.inE().forEach(FunctionUtils.wrapConsumer(jsonGenerator::writeObject));
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndObject();
        }
    }
}
