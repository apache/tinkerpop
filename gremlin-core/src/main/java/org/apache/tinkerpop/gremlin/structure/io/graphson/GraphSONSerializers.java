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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class used to serialize graph-based objects such as vertices, edges, properties, and paths. These serializers
 * present a generalized way to serialize the implementations of core interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphSONSerializers {
    static class VertexPropertyJacksonSerializer extends StdSerializer<VertexProperty> {

        private final boolean normalize;

        public VertexPropertyJacksonSerializer(final boolean normalize) {
            super(VertexProperty.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final VertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, null, normalize);
        }

        @Override
        public void serializeWithType(final VertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serializerVertexProperty(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
        }
    }

    static class PropertyJacksonSerializer extends StdSerializer<Property> {

        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator, serializerProvider, null);
        }
        @Override
        public void serializeWithType(final Property property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator, serializerProvider, typeSerializer);
        }

        private static void ser(final Property property, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            serializerProvider.defaultSerializeField(GraphSONTokens.KEY, property.key(), jsonGenerator);
            serializerProvider.defaultSerializeField(GraphSONTokens.VALUE, property.value(), jsonGenerator);
            jsonGenerator.writeEndObject();
        }
    }

    static class EdgeJacksonSerializer extends StdSerializer<Edge> {

        private final boolean normalize;

        public EdgeJacksonSerializer(final boolean normalize) {
            super(Edge.class);
            this.normalize = normalize;
        }


        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(edge, jsonGenerator, serializerProvider, null);
        }
        @Override
        public void serializeWithType(final Edge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(edge, jsonGenerator, serializerProvider, typeSerializer);
        }

        private void ser(final Edge edge, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            GraphSONUtil.writeWithType(GraphSONTokens.ID, edge.id(), jsonGenerator, serializerProvider, typeSerializer);

            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.label());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.EDGE);
            jsonGenerator.writeStringField(GraphSONTokens.IN_LABEL, edge.inVertex().label());
            jsonGenerator.writeStringField(GraphSONTokens.OUT_LABEL, edge.outVertex().label());
            GraphSONUtil.writeWithType(GraphSONTokens.IN, edge.inVertex().id(), jsonGenerator, serializerProvider, typeSerializer);
            GraphSONUtil.writeWithType(GraphSONTokens.OUT, edge.outVertex().id(), jsonGenerator, serializerProvider, typeSerializer);
            writeProperties(edge, jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final Edge edge, final JsonGenerator jsonGenerator,
                                            final SerializerProvider serializerProvider,
                                            final TypeSerializer typeSerializer) throws IOException {
            final Iterator<Property<Object>> elementProperties = normalize ?
                    IteratorUtils.list(edge.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : edge.properties();
            if (elementProperties.hasNext()) {
                jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
                if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
                while (elementProperties.hasNext()) {
                    final Property<Object> elementProperty = elementProperties.next();
                    GraphSONUtil.writeWithType(elementProperty.key(), elementProperty.value(), jsonGenerator, serializerProvider, typeSerializer);
                }
                jsonGenerator.writeEndObject();
            }
        }

    }

    static class VertexJacksonSerializer extends StdSerializer<Vertex> {

        private final boolean normalize;

        public VertexJacksonSerializer(final boolean normalize) {
            super(Vertex.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, null);
        }

        @Override
        public void serializeWithType(final Vertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(vertex, jsonGenerator, serializerProvider, typeSerializer);

        }

        private void ser(final Vertex vertex, final JsonGenerator jsonGenerator,
                                final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            GraphSONUtil.writeWithType(GraphSONTokens.ID, vertex.id(), jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.label());
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);
            writeProperties(vertex, jsonGenerator, serializerProvider, typeSerializer);
            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final Vertex vertex, final JsonGenerator jsonGenerator,
                                            final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            final Iterator<VertexProperty<Object>> vertexProperties = normalize ?
                    IteratorUtils.list(vertex.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : vertex.properties();
            if (vertexProperties.hasNext()) {
                jsonGenerator.writeArrayFieldStart(GraphSONTokens.PROPERTIES);
                if (typeSerializer != null) {
                    jsonGenerator.writeString(ArrayList.class.getName());
                    jsonGenerator.writeStartArray();
                }

                while (vertexProperties.hasNext()) {
                    jsonGenerator.writeObject(vertexProperties.next());
                }
                jsonGenerator.writeEndArray();
                if (typeSerializer != null) jsonGenerator.writeEndArray();
            }
        }

    }

    static class PathJacksonSerializer extends StdSerializer<Path> {

        public PathJacksonSerializer() {
            super(Path.class);
        }
        @Override
        public void serialize(final Path path, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(path, jsonGenerator, null);
        }

        @Override
        public void serializeWithType(final Path path, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            ser(path, jsonGenerator, typeSerializer);
        }
        private static void ser(final Path path, final JsonGenerator jsonGenerator, final TypeSerializer typeSerializer)
                throws IOException {
            jsonGenerator.writeStartObject();
            if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
            jsonGenerator.writeObjectField(GraphSONTokens.LABELS, path.labels());
            jsonGenerator.writeObjectField(GraphSONTokens.OBJECTS, path.objects());
            jsonGenerator.writeEndObject();
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
                jsonGenerator.writeFieldName((((Element) o).id()).toString());
            else
                super.serialize(o, jsonGenerator, serializerProvider);
        }
    }

    static class TraversalMetricsJacksonSerializer extends StdSerializer<TraversalMetrics> {
        public TraversalMetricsJacksonSerializer() {
            super(TraversalMetrics.class);
        }

        @Override
        public void serialize(final TraversalMetrics property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            serializeInternal(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final TraversalMetrics property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            serializeInternal(property, jsonGenerator);
        }

        private static void serializeInternal(final TraversalMetrics traversalMetrics, final JsonGenerator jsonGenerator) throws IOException {
            // creation of the map enables all the fields to be properly written with their type if required
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.DURATION, traversalMetrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);
            final List<Map<String, Object>> metrics = new ArrayList<>();
            traversalMetrics.getMetrics().forEach(it -> metrics.add(metricsToMap(it)));
            m.put(GraphSONTokens.METRICS, metrics);

            jsonGenerator.writeObject(m);
        }

        private static Map<String, Object> metricsToMap(final Metrics metrics) {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, metrics.getId());
            m.put(GraphSONTokens.NAME, metrics.getName());
            m.put(GraphSONTokens.COUNTS, metrics.getCounts());
            m.put(GraphSONTokens.DURATION, metrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);

            if (!metrics.getAnnotations().isEmpty()) {
                m.put(GraphSONTokens.ANNOTATIONS, metrics.getAnnotations());
            }

            if (!metrics.getNested().isEmpty()) {
                final List<Map<String, Object>> nested = new ArrayList<>();
                metrics.getNested().forEach(it -> nested.add(metricsToMap(it)));
                m.put(GraphSONTokens.METRICS, nested);
            }
            return m;
        }

    }

    private static void serializerVertexProperty(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                 final SerializerProvider serializerProvider,
                                                 final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        jsonGenerator.writeStartObject();
        if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
        GraphSONUtil.writeWithType(GraphSONTokens.ID, property.id(), jsonGenerator, serializerProvider, typeSerializer);
        GraphSONUtil.writeWithType(GraphSONTokens.VALUE, property.value(), jsonGenerator, serializerProvider, typeSerializer);
        jsonGenerator.writeStringField(GraphSONTokens.LABEL, property.label());
        tryWriteMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
        jsonGenerator.writeEndObject();
    }

    private static void tryWriteMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                               final SerializerProvider serializerProvider,
                                               final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        // when "detached" you can't check features of the graph it detached from so it has to be
        // treated differently from a regular VertexProperty implementation.
        if (property instanceof DetachedVertexProperty) {
            // only write meta properties key if they exist
            if (property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        } else {
            // still attached - so we can check the features to see if it's worth even trying to write the
            // meta properties key
            if (property.graph().features().vertex().supportsMetaProperties() && property.properties().hasNext()) {
                writeMetaProperties(property, jsonGenerator, serializerProvider, typeSerializer, normalize);
            }
        }
    }

    private static void writeMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                            final SerializerProvider serializerProvider,
                                            final TypeSerializer typeSerializer, final boolean normalize) throws IOException {
        jsonGenerator.writeObjectFieldStart(GraphSONTokens.PROPERTIES);
        if (typeSerializer != null) jsonGenerator.writeStringField(GraphSONTokens.CLASS, HashMap.class.getName());
        final Iterator<Property<Object>> metaProperties = normalize ?
                IteratorUtils.list(( Iterator<Property<Object>>) property.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : property.properties();
        while (metaProperties.hasNext()) {
            final Property<Object> metaProperty = metaProperties.next();
            GraphSONUtil.writeWithType(metaProperty.key(), metaProperty.value(), jsonGenerator, serializerProvider, typeSerializer);
        }
        jsonGenerator.writeEndObject();
    }

}
