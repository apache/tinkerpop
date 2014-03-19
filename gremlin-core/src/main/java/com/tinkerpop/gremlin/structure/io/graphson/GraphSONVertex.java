package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;

import java.io.IOException;

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
        public VertexJacksonSerializer() {
            super(GraphSONVertex.class);
        }

        @Override
        public void serialize(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            final Vertex vertex = directionalVertex.getVertexToSerialize();
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONModule.TOKEN_ID, vertex.getId());
            jsonGenerator.writeStringField(GraphSONModule.TOKEN_LABEL, vertex.getLabel());
            jsonGenerator.writeStringField(GraphSONModule.TOKEN_TYPE, GraphSONModule.TOKEN_VERTEX);
            jsonGenerator.writeObjectField(GraphSONModule.TOKEN_PROPERTIES, vertex.getProperties());

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.OUT) {
                jsonGenerator.writeArrayFieldStart(GraphSONModule.TOKEN_OUT);
                vertex.outE().forEach(e -> {
                    try {
                        jsonGenerator.writeObject(e);
                    } catch (IOException ioe) {
                        // todo: what do we do here?
                    }
                });
                jsonGenerator.writeEndArray();
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.IN) {
                jsonGenerator.writeArrayFieldStart(GraphSONModule.TOKEN_IN);
                vertex.inE().forEach(e -> {
                    try {
                        jsonGenerator.writeObject(e);
                    } catch (IOException ioe) {
                        // todo: what do we do here?
                    }
                });
                jsonGenerator.writeEndArray();
            }
            jsonGenerator.writeEndObject();
        }
    }
}
