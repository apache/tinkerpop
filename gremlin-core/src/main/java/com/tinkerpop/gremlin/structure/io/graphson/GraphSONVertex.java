package com.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.util.StreamFactory;

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

        public VertexJacksonSerializer() {
            super(GraphSONVertex.class);
        }

        @Override
        public void serialize(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(directionalVertex, jsonGenerator);
        }

        @Override
        public void serializeWithType(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(directionalVertex, jsonGenerator);
        }

        public void ser(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator) throws IOException {
            final Vertex vertex = directionalVertex.getVertexToSerialize();
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.id());
            m.put(GraphSONTokens.LABEL, vertex.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            final Object properties = StreamFactory.stream(vertex.iterators().propertyIterator())
                    .collect(Collectors.groupingBy(vp -> vp.key()));
            final Object hiddens = StreamFactory.stream(vertex.iterators().hiddenPropertyIterator())
                    .collect(Collectors.groupingBy(vp -> vp.key()));
            m.put(GraphSONTokens.PROPERTIES, properties);
            m.put(GraphSONTokens.HIDDENS, hiddens);

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.OUT) {
                m.put(GraphSONTokens.OUT_E, StreamFactory.stream(vertex.iterators().edgeIterator(Direction.OUT)).collect(Collectors.toList()));
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.IN) {
                m.put(GraphSONTokens.IN_E, StreamFactory.stream(vertex.iterators().edgeIterator(Direction.IN)).collect(Collectors.toList()));
            }

            jsonGenerator.writeObject(m);
        }
    }
}
