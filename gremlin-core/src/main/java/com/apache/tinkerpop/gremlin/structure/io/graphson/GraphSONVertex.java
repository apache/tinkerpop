/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.apache.tinkerpop.gremlin.structure.Direction;
import com.apache.tinkerpop.gremlin.structure.Property;
import com.apache.tinkerpop.gremlin.structure.Vertex;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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

        public static void ser(final GraphSONVertex directionalVertex, final JsonGenerator jsonGenerator) throws IOException {
            final Vertex vertex = directionalVertex.getVertexToSerialize();
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.id());
            m.put(GraphSONTokens.LABEL, vertex.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            final Object properties = IteratorUtils.groupBy(vertex.iterators().propertyIterator(), Property::key);
            m.put(GraphSONTokens.PROPERTIES, properties);

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.OUT) {
                m.put(GraphSONTokens.OUT_E, IteratorUtils.fill(vertex.iterators().edgeIterator(Direction.OUT), new ArrayList()));
            }

            if (directionalVertex.getDirection() == Direction.BOTH || directionalVertex.getDirection() == Direction.IN) {
                m.put(GraphSONTokens.IN_E, IteratorUtils.fill(vertex.iterators().edgeIterator(Direction.IN), new ArrayList()));
            }

            jsonGenerator.writeObject(m);
        }
    }
}
