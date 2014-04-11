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
import java.util.HashMap;
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
            final Map<String,Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.getId());
            m.put(GraphSONTokens.LABEL, vertex.getLabel());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);
            m.put(GraphSONTokens.PROPERTIES,
                    vertex.getProperties().values().stream().collect(
                            Collectors.toMap(Property::getKey, p -> (p.get() instanceof AnnotatedList) ? IOAnnotatedList.from((AnnotatedList) p.get()) : p.get())));

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.OUT) {
                m.put(GraphSONTokens.OUT, vertex.outE().toList());
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.IN) {
                m.put(GraphSONTokens.IN, vertex.inE().toList());
            }

            jsonGenerator.writeObject(m);
        }
    }
}
