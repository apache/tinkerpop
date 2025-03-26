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
package org.apache.tinkerpop.gremlin.structure.util.star;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class StarGraphGraphSONSerializerV4 extends StdSerializer<DirectionalStarGraph> {
    private final boolean normalize;
    public StarGraphGraphSONSerializerV4(final boolean normalize) {
        super(DirectionalStarGraph.class);
        this.normalize = normalize;
    }

    @Override
    public void serialize(final DirectionalStarGraph starGraph, final JsonGenerator jsonGenerator,
                          final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
        ser(starGraph, jsonGenerator, serializerProvider, null);
    }

    @Override
    public void serializeWithType(final DirectionalStarGraph starGraph, final JsonGenerator jsonGenerator,
                                  final SerializerProvider serializerProvider,
                                  final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
        ser(starGraph, jsonGenerator, serializerProvider, typeSerializer);
    }

    private void ser(final DirectionalStarGraph directionalStarGraph, final JsonGenerator jsonGenerator,
                     final SerializerProvider serializerProvider,
                     final TypeSerializer typeSerializer) throws IOException, JsonProcessingException {
        final StarGraph starGraph = directionalStarGraph.getStarGraphToSerialize();
        GraphSONUtil.writeStartObject(starGraph, jsonGenerator, typeSerializer);
        GraphSONUtil.writeWithType(GraphSONTokens.ID, starGraph.starVertex.id, jsonGenerator, serializerProvider, typeSerializer);
        jsonGenerator.writeStringField(GraphSONTokens.LABEL, starGraph.starVertex.label);
        if (directionalStarGraph.getDirection() != null) writeEdges(directionalStarGraph, jsonGenerator, serializerProvider, typeSerializer, Direction.IN);
        if (directionalStarGraph.getDirection() != null) writeEdges(directionalStarGraph, jsonGenerator, serializerProvider, typeSerializer, Direction.OUT);
        if (starGraph.starVertex.vertexProperties != null && !starGraph.starVertex.vertexProperties.isEmpty()) {
            jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
            GraphSONUtil.writeStartObject(starGraph, jsonGenerator, typeSerializer);
            final Set<String> keys = normalize ? new TreeSet<>(starGraph.starVertex.vertexProperties.keySet()) : starGraph.starVertex.vertexProperties.keySet();
            for (final String k : keys) {
                final List<VertexProperty> vp = starGraph.starVertex.vertexProperties.get(k);
                jsonGenerator.writeFieldName(k);
                GraphSONUtil.writeStartArray(k, jsonGenerator, typeSerializer);

                final List<VertexProperty> vertexProperties = normalize ?sort(vp, Comparators.PROPERTY_COMPARATOR) : vp;
                for (final VertexProperty property : vertexProperties) {
                    GraphSONUtil.writeStartObject(property, jsonGenerator, typeSerializer);
                    GraphSONUtil.writeWithType(GraphSONTokens.ID, property.id(), jsonGenerator, serializerProvider, typeSerializer);
                    GraphSONUtil.writeWithType(GraphSONTokens.VALUE, property.value(), jsonGenerator, serializerProvider, typeSerializer);

                    final Iterator<Property> metaProperties = normalize ?
                            IteratorUtils.list(property.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : property.properties();
                    if (metaProperties.hasNext()) {
                        jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
                        GraphSONUtil.writeStartObject(metaProperties, jsonGenerator, typeSerializer);

                        while (metaProperties.hasNext()) {
                            final Property<Object> meta = metaProperties.next();
                            GraphSONUtil.writeWithType(meta.key(), meta.value(), jsonGenerator, serializerProvider, typeSerializer);
                        }
                        GraphSONUtil.writeEndObject(metaProperties, jsonGenerator, typeSerializer);
                    }
                    GraphSONUtil.writeEndObject(property, jsonGenerator, typeSerializer);
                }
                GraphSONUtil.writeEndArray(k, jsonGenerator, typeSerializer);
            }
            GraphSONUtil.writeEndObject(starGraph, jsonGenerator, typeSerializer);
        }
        GraphSONUtil.writeEndObject(starGraph, jsonGenerator, typeSerializer);
    }

    private void writeEdges(final DirectionalStarGraph directionalStarGraph, final JsonGenerator jsonGenerator,
                            final SerializerProvider serializerProvider,
                            final TypeSerializer typeSerializer,
                            final Direction direction)  throws IOException, JsonProcessingException {
        // only write edges if there are some AND if the user requested them to be serialized AND if they match
        // the direction being serialized by the format
        final StarGraph starGraph = directionalStarGraph.getStarGraphToSerialize();
        final Direction edgeDirectionToSerialize = directionalStarGraph.getDirection();
        final Map<String, List<Edge>> starEdges = direction.equals(Direction.OUT) ? starGraph.starVertex.outEdges : starGraph.starVertex.inEdges;
        final boolean writeEdges = null != starEdges && edgeDirectionToSerialize != null
                && (edgeDirectionToSerialize == direction || edgeDirectionToSerialize == Direction.BOTH);
        if (writeEdges) {
            jsonGenerator.writeFieldName(direction == Direction.IN ? GraphSONTokens.IN_E : GraphSONTokens.OUT_E);
            GraphSONUtil.writeStartObject(directionalStarGraph, jsonGenerator, typeSerializer);
            final Set<String> keys = normalize ? new TreeSet<>(starEdges.keySet()) : starEdges.keySet();
            for (final String k : keys) {
                final List<Edge> edges = starEdges.get(k);
                jsonGenerator.writeFieldName(k);
                GraphSONUtil.writeStartArray(k, jsonGenerator, typeSerializer);

                final List<Edge> edgesToWrite = normalize ? sort(edges, Comparators.EDGE_COMPARATOR) : edges;
                for (final Edge edge : edgesToWrite) {
                    GraphSONUtil.writeStartObject(edge, jsonGenerator, typeSerializer);
                    GraphSONUtil.writeWithType(GraphSONTokens.ID, edge.id(), jsonGenerator, serializerProvider, typeSerializer);
                    GraphSONUtil.writeWithType(direction.equals(Direction.OUT) ? GraphSONTokens.IN : GraphSONTokens.OUT,
                            direction.equals(Direction.OUT) ? edge.inVertex().id() : edge.outVertex().id(),
                            jsonGenerator, serializerProvider, typeSerializer);

                    final Iterator<Property<Object>> edgeProperties = normalize ?
                            IteratorUtils.list(edge.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : edge.properties();
                    if (edgeProperties.hasNext()) {
                        jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
                        GraphSONUtil.writeStartObject(edge, jsonGenerator, typeSerializer);
                        while (edgeProperties.hasNext()) {
                            final Property<Object> meta = edgeProperties.next();
                            GraphSONUtil.writeWithType(meta.key(), meta.value(), jsonGenerator, serializerProvider, typeSerializer);
                        }
                        GraphSONUtil.writeEndObject(edge, jsonGenerator, typeSerializer);
                    }
                    GraphSONUtil.writeEndObject(edge, jsonGenerator, typeSerializer);
                }
                GraphSONUtil.writeEndArray(k, jsonGenerator, typeSerializer);
            }
            GraphSONUtil.writeEndObject(directionalStarGraph, jsonGenerator, typeSerializer);
        }
    }

    private static <S> List<S> sort(final List<S> listToSort, final Comparator comparator) {
        Collections.sort(listToSort, comparator);
        return listToSort;
    }

}
