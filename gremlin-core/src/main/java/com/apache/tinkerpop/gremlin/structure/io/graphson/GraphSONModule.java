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

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdKeySerializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.apache.tinkerpop.gremlin.process.Path;
import com.apache.tinkerpop.gremlin.process.util.metric.Metrics;
import com.apache.tinkerpop.gremlin.process.util.metric.TraversalMetrics;
import com.apache.tinkerpop.gremlin.structure.*;
import com.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import com.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONModule extends SimpleModule {

    public GraphSONModule(final boolean normalize) {
        super("graphson");
        addSerializer(Edge.class, new EdgeJacksonSerializer());
        addSerializer(Vertex.class, new VertexJacksonSerializer());
        addSerializer(GraphSONVertex.class, new GraphSONVertex.VertexJacksonSerializer());
        addSerializer(GraphSONGraph.class, new GraphSONGraph.GraphJacksonSerializer(normalize));
        addSerializer(GraphSONVertexProperty.class, new GraphSONVertexProperty.GraphSONVertexPropertySerializer());
        addSerializer(VertexProperty.class, new VertexPropertyJacksonSerializer());
        addSerializer(Property.class, new PropertyJacksonSerializer());
        addSerializer(TraversalMetrics.class, new TraversalMetricsJacksonSerializer());
        addSerializer(Path.class, new PathJacksonSerializer());
    }

    static class VertexPropertyJacksonSerializer extends StdSerializer<VertexProperty> {
        public VertexPropertyJacksonSerializer() {
            super(VertexProperty.class);
        }

        @Override
        public void serialize(final VertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final VertexProperty property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator);
        }

        private static void ser(final VertexProperty property, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, property.id());
            m.put(GraphSONTokens.VALUE, property.value());
            m.put(GraphSONTokens.LABEL, property.label());
            m.put(GraphSONTokens.PROPERTIES, props(property));

            jsonGenerator.writeObject(m);
        }

        private static Map<String, Object> props(final VertexProperty property) {
            if (property instanceof DetachedVertexProperty) {
                try {
                    return IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value);
                } catch (UnsupportedOperationException uoe) {
                    return new HashMap<>();
                }
            } else {
                return (property.graph().features().vertex().supportsMetaProperties()) ?
                        IteratorUtils.collectMap(property.iterators().propertyIterator(), Property::key, Property::value) :
                        new HashMap<>();
            }
        }
    }

    static class PropertyJacksonSerializer extends StdSerializer<Property> {
        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(property, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Property property, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(property, jsonGenerator);
        }

        private static void ser(final Property property, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.VALUE, property.value());
            jsonGenerator.writeObject(m);
        }
    }


    static class EdgeJacksonSerializer extends StdSerializer<Edge> {
        public EdgeJacksonSerializer() {
            super(Edge.class);
        }

        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(edge, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Edge edge, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(edge, jsonGenerator);
        }

        private static void ser(final Edge edge, final JsonGenerator jsonGenerator) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, edge.id());
            m.put(GraphSONTokens.LABEL, edge.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.EDGE);

            final Map<String, Object> properties = IteratorUtils.collectMap(edge.iterators().propertyIterator(), Property::key, Property::value);
            m.put(GraphSONTokens.PROPERTIES, properties);

            final Vertex inV = edge.iterators().vertexIterator(Direction.IN).next();
            m.put(GraphSONTokens.IN, inV.id());
            m.put(GraphSONTokens.IN_LABEL, inV.label());

            final Vertex outV = edge.iterators().vertexIterator(Direction.OUT).next();
            m.put(GraphSONTokens.OUT, outV.id());
            m.put(GraphSONTokens.OUT_LABEL, outV.label());

            jsonGenerator.writeObject(m);
        }
    }

    static class VertexJacksonSerializer extends StdSerializer<Vertex> {

        public VertexJacksonSerializer() {
            super(Vertex.class);
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            ser(vertex, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Vertex vertex, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer) throws IOException {
            ser(vertex, jsonGenerator);

        }

        private static void ser(final Vertex vertex, final JsonGenerator jsonGenerator)
                throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, vertex.id());
            m.put(GraphSONTokens.LABEL, vertex.label());
            m.put(GraphSONTokens.TYPE, GraphSONTokens.VERTEX);

            // convert to GraphSONVertexProperty so that the label does not get serialized in the output - it is
            // redundant because the key in the map is the same as the label.
            final Iterator<GraphSONVertexProperty> vertexPropertyList = IteratorUtils.map(vertex.iterators().propertyIterator(), GraphSONVertexProperty::new);
            final Object properties = IteratorUtils.groupBy(vertexPropertyList, vp -> vp.getToSerialize().key());
            m.put(GraphSONTokens.PROPERTIES, properties);

            jsonGenerator.writeObject(m);
        }
    }

    static class PathJacksonSerializer extends StdSerializer<Path> {
        public PathJacksonSerializer() {
            super(Path.class);
        }

        @Override
        public void serialize(final Path path, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            ser(path, jsonGenerator);
        }

        @Override
        public void serializeWithType(final Path path, final JsonGenerator jsonGenerator,
                                      final SerializerProvider serializerProvider, final TypeSerializer typeSerializer)
                throws IOException, JsonProcessingException {
            ser(path, jsonGenerator);
        }

        private static void ser(final Path path, final JsonGenerator jsonGenerator)
                throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.LABELS, path.labels());
            m.put(GraphSONTokens.OBJECTS, path.objects());
            jsonGenerator.writeObject(m);
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
            final Map<String, Object> m = new HashMap<>();

            m.put(GraphSONTokens.DURATION, traversalMetrics.getDuration(TimeUnit.MILLISECONDS));
            List<Map<String, Object>> metrics = new ArrayList<>();
            traversalMetrics.getMetrics().forEach(it -> metrics.add(metricsToMap(it)));
            m.put(GraphSONTokens.METRICS, metrics);

            jsonGenerator.writeObject(m);
        }

        private static Map<String, Object> metricsToMap(final Metrics metrics) {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, metrics.getId());
            m.put(GraphSONTokens.NAME, metrics.getName());
            m.put(GraphSONTokens.COUNT, metrics.getCount());
            m.put(GraphSONTokens.DURATION, metrics.getDuration(TimeUnit.MILLISECONDS));
            m.put(GraphSONTokens.PERCENT_DURATION, metrics.getPercentDuration());

            if (!metrics.getAnnotations().isEmpty()) {
                m.put(GraphSONTokens.ANNOTATIONS, metrics.getAnnotations());
            }

            if (!metrics.getNested().isEmpty()) {
                List<Map<String, Object>> nested = new ArrayList<>();
                metrics.getNested().forEach(it -> nested.add(metricsToMap(it)));
                m.put(GraphSONTokens.METRICS, nested);
            }
            return m;
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
}
