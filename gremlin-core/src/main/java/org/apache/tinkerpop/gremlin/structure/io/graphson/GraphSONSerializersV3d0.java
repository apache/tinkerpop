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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.Comparators;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerationException;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdKeySerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.type.TypeFactory;
import org.javatuples.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONUtil.safeWriteObjectField;

/**
 * GraphSON serializers for graph-based objects such as vertices, edges, properties, and paths. These serializers
 * present a generalized way to serialize the implementations of core interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphSONSerializersV3d0 {

    private GraphSONSerializersV3d0() {
    }

    ////////////////////////////// SERIALIZERS /////////////////////////////////

    final static class VertexJacksonSerializer extends StdScalarSerializer<Vertex> {

        private final boolean normalize;

        public VertexJacksonSerializer(final boolean normalize) {
            super(Vertex.class);
            this.normalize = normalize;
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, vertex.id());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, vertex.label());
            writeProperties(vertex, jsonGenerator);

            jsonGenerator.writeEndObject();

        }

        private void writeProperties(final Vertex vertex, final JsonGenerator jsonGenerator) throws IOException {
            if (vertex.keys().size() == 0)
                return;
            jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
            jsonGenerator.writeStartObject();

            final List<String> keys = normalize ?
                    IteratorUtils.list(vertex.keys().iterator(), Comparator.naturalOrder()) : new ArrayList<>(vertex.keys());
            for (String key : keys) {
                final Iterator<VertexProperty<Object>> vertexProperties = normalize ?
                        IteratorUtils.list(vertex.properties(key), Comparators.PROPERTY_COMPARATOR).iterator() : vertex.properties(key);
                if (vertexProperties.hasNext()) {
                    jsonGenerator.writeFieldName(key);

                    jsonGenerator.writeStartArray();
                    while (vertexProperties.hasNext()) {
                        jsonGenerator.writeObject(vertexProperties.next());
                    }
                    jsonGenerator.writeEndArray();
                }
            }

            jsonGenerator.writeEndObject();
        }
    }

    final static class EdgeJacksonSerializer extends StdScalarSerializer<Edge> {

        private final boolean normalize;

        public EdgeJacksonSerializer(final boolean normalize) {
            super(Edge.class);
            this.normalize = normalize;
        }


        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, edge.id());
            jsonGenerator.writeStringField(GraphSONTokens.LABEL, edge.label());
            jsonGenerator.writeStringField(GraphSONTokens.IN_LABEL, edge.inVertex().label());
            jsonGenerator.writeStringField(GraphSONTokens.OUT_LABEL, edge.outVertex().label());
            jsonGenerator.writeObjectField(GraphSONTokens.IN, edge.inVertex().id());
            jsonGenerator.writeObjectField(GraphSONTokens.OUT, edge.outVertex().id());
            writeProperties(edge, jsonGenerator);

            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final Edge edge, final JsonGenerator jsonGenerator) throws IOException {
            final Iterator<Property<Object>> elementProperties = normalize ?
                    IteratorUtils.list(edge.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : edge.properties();
            if (elementProperties.hasNext()) {
                jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);

                jsonGenerator.writeStartObject();
                elementProperties.forEachRemaining(prop -> safeWriteObjectField(jsonGenerator, prop.key(), prop));
                jsonGenerator.writeEndObject();
            }
        }
    }

    final static class PropertyJacksonSerializer extends StdScalarSerializer<Property> {

        public PropertyJacksonSerializer() {
            super(Property.class);
        }

        @Override
        public void serialize(final Property property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONTokens.KEY, property.key());
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, property.value());
            jsonGenerator.writeEndObject();
        }
    }

    final static class VertexPropertyJacksonSerializer extends StdScalarSerializer<VertexProperty> {

        private final boolean normalize;
        private final boolean includeLabel;

        public VertexPropertyJacksonSerializer(final boolean normalize, final boolean includeLabel) {
            super(VertexProperty.class);
            this.normalize = normalize;
            this.includeLabel = includeLabel;
        }

        @Override
        public void serialize(final VertexProperty property, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, property.id());
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, property.value());
            if (includeLabel)
                jsonGenerator.writeStringField(GraphSONTokens.LABEL, property.label());
            tryWriteMetaProperties(property, jsonGenerator, normalize);

            jsonGenerator.writeEndObject();
        }

        private static void tryWriteMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                   final boolean normalize) throws IOException {
            // when "detached" you can't check features of the graph it detached from so it has to be
            // treated differently from a regular VertexProperty implementation.
            if (property instanceof DetachedVertexProperty) {
                // only write meta properties key if they exist
                if (property.properties().hasNext()) {
                    writeMetaProperties(property, jsonGenerator, normalize);
                }
            } else {
                // still attached - so we can check the features to see if it's worth even trying to write the
                // meta properties key
                if (property.graph().features().vertex().supportsMetaProperties() && property.properties().hasNext()) {
                    writeMetaProperties(property, jsonGenerator, normalize);
                }
            }
        }

        private static void writeMetaProperties(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                final boolean normalize) throws IOException {
            jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
            jsonGenerator.writeStartObject();

            final Iterator<Property<Object>> metaProperties = normalize ?
                    IteratorUtils.list((Iterator<Property<Object>>) property.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : property.properties();
            while (metaProperties.hasNext()) {
                final Property<Object> metaProperty = metaProperties.next();
                jsonGenerator.writeObjectField(metaProperty.key(), metaProperty.value());
            }

            jsonGenerator.writeEndObject();
        }
    }

    final static class PathJacksonSerializer extends StdScalarSerializer<Path> {

        public PathJacksonSerializer() {
            super(Path.class);
        }

        @Override
        public void serialize(final Path path, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException, JsonGenerationException {
            jsonGenerator.writeStartObject();

            // paths shouldn't serialize with properties if the path contains graph elements
            final Path p = DetachedFactory.detach(path, false);
            jsonGenerator.writeObjectField(GraphSONTokens.LABELS, p.labels());
            jsonGenerator.writeObjectField(GraphSONTokens.OBJECTS, p.objects());

            jsonGenerator.writeEndObject();
        }
    }

    final static class TreeJacksonSerializer extends StdScalarSerializer<Tree> {

        public TreeJacksonSerializer() {
            super(Tree.class);
        }

        @Override
        public void serialize(final Tree tree, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException, JsonGenerationException {
            jsonGenerator.writeStartArray();
            final Set<Map.Entry<Element, Tree>> set = tree.entrySet();
            for (Map.Entry<Element, Tree> entry : set) {
                jsonGenerator.writeStartObject();
                jsonGenerator.writeObjectField(GraphSONTokens.KEY, entry.getKey());
                jsonGenerator.writeObjectField(GraphSONTokens.VALUE, entry.getValue());
                jsonGenerator.writeEndObject();
            }
            jsonGenerator.writeEndArray();
        }
    }

    final static class TraversalExplanationJacksonSerializer extends StdSerializer<TraversalExplanation> {
        public TraversalExplanationJacksonSerializer() {
            super(TraversalExplanation.class);
        }

        @Override
        public void serialize(final TraversalExplanation traversalExplanation, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ORIGINAL, getStepsAsList(traversalExplanation.getOriginalTraversal()));

            final List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> strategyTraversals = traversalExplanation.getStrategyTraversals();

            final List<Map<String, Object>> intermediates = new ArrayList<>();
            for (final Pair<TraversalStrategy, Traversal.Admin<?, ?>> pair : strategyTraversals) {
                final Map<String, Object> intermediate = new HashMap<>();
                intermediate.put(GraphSONTokens.STRATEGY, pair.getValue0().toString());
                intermediate.put(GraphSONTokens.CATEGORY, pair.getValue0().getTraversalCategory().getSimpleName());
                intermediate.put(GraphSONTokens.TRAVERSAL, getStepsAsList(pair.getValue1()));
                intermediates.add(intermediate);
            }
            m.put(GraphSONTokens.INTERMEDIATE, intermediates);

            if (strategyTraversals.isEmpty())
                m.put(GraphSONTokens.FINAL, getStepsAsList(traversalExplanation.getOriginalTraversal()));
            else
                m.put(GraphSONTokens.FINAL, getStepsAsList(strategyTraversals.get(strategyTraversals.size() - 1).getValue1()));

            jsonGenerator.writeObject(m);
        }

        private List<String> getStepsAsList(final Traversal.Admin<?, ?> t) {
            final List<String> steps = new ArrayList<>();
            t.getSteps().iterator().forEachRemaining(s -> steps.add(s.toString()));
            return steps;
        }
    }

    final static class IntegerGraphSONSerializer extends StdScalarSerializer<Integer> {
        public IntegerGraphSONSerializer() {
            super(Integer.class);
        }

        @Override
        public void serialize(final Integer integer, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(((Integer) integer).intValue());
        }
    }

    final static class DoubleGraphSONSerializer extends StdScalarSerializer<Double> {
        public DoubleGraphSONSerializer() {
            super(Double.class);
        }

        @Override
        public void serialize(final Double doubleValue, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeNumber(doubleValue);
        }
    }

    final static class TraversalMetricsJacksonSerializer extends StdScalarSerializer<TraversalMetrics> {
        public TraversalMetricsJacksonSerializer() {
            super(TraversalMetrics.class);
        }

        @Override
        public void serialize(final TraversalMetrics traversalMetrics, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            // creation of the map enables all the fields to be properly written with their type if required
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.DURATION, traversalMetrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);
            final List<Metrics> metrics = new ArrayList<>();
            metrics.addAll(traversalMetrics.getMetrics());
            m.put(GraphSONTokens.METRICS, metrics);

            jsonGenerator.writeObject(m);
        }
    }

    final static class MetricsJacksonSerializer extends StdScalarSerializer<Metrics> {
        public MetricsJacksonSerializer() {
            super(Metrics.class);
        }

        @Override
        public void serialize(final Metrics metrics, final JsonGenerator jsonGenerator,
                              final SerializerProvider serializerProvider) throws IOException {
            final Map<String, Object> m = new HashMap<>();
            m.put(GraphSONTokens.ID, metrics.getId());
            m.put(GraphSONTokens.NAME, metrics.getName());
            m.put(GraphSONTokens.COUNTS, metrics.getCounts());
            m.put(GraphSONTokens.DURATION, metrics.getDuration(TimeUnit.NANOSECONDS) / 1000000d);

            if (!metrics.getAnnotations().isEmpty()) {
                m.put(GraphSONTokens.ANNOTATIONS, metrics.getAnnotations());
            }
            if (!metrics.getNested().isEmpty()) {
                final List<Metrics> nested = new ArrayList<>();
                metrics.getNested().forEach(it -> nested.add(it));
                m.put(GraphSONTokens.METRICS, nested);
            }
            jsonGenerator.writeObject(m);
        }
    }


    /**
     * Maps in the JVM can have {@link Object} as a key, but in JSON they must be a {@link String}.
     */
    final static class GraphSONKeySerializer extends StdKeySerializer {

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


    //////////////////////////// DESERIALIZERS ///////////////////////////

    static class VertexJacksonDeserializer extends StdDeserializer<Vertex> {

        public VertexJacksonDeserializer() {
            super(Vertex.class);
        }

        public Vertex deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final DetachedVertex.Builder v = DetachedVertex.build();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.ID)) {
                    jsonParser.nextToken();
                    v.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.LABEL)) {
                    jsonParser.nextToken();
                    v.setLabel(jsonParser.getText());
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.PROPERTIES)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        jsonParser.nextToken();
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            v.addProperty((DetachedVertexProperty) deserializationContext.readValue(jsonParser, VertexProperty.class));
                        }
                    }
                }
            }

            return v.create();
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class EdgeJacksonDeserializer extends StdDeserializer<Edge> {

        public EdgeJacksonDeserializer() {
            super(Edge.class);
        }

        @Override
        public Edge deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final DetachedEdge.Builder e = DetachedEdge.build();
            final DetachedVertex.Builder inV = DetachedVertex.build();
            final DetachedVertex.Builder outV = DetachedVertex.build();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.ID)) {
                    jsonParser.nextToken();
                    e.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.LABEL)) {
                    jsonParser.nextToken();
                    e.setLabel(jsonParser.getText());
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.OUT)) {
                    jsonParser.nextToken();
                    outV.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.OUT_LABEL)) {
                    jsonParser.nextToken();
                    outV.setLabel(jsonParser.getText());
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.IN)) {
                    jsonParser.nextToken();
                    inV.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.IN_LABEL)) {
                    jsonParser.nextToken();
                    inV.setLabel(jsonParser.getText());
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.PROPERTIES)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        jsonParser.nextToken();
                        e.addProperty(deserializationContext.readValue(jsonParser, Property.class));
                    }
                }
            }

            e.setInV(inV.create());
            e.setOutV(outV.create());

            return e.create();
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class PropertyJacksonDeserializer extends StdDeserializer<Property> {

        public PropertyJacksonDeserializer() {
            super(Property.class);
        }

        @Override
        public Property deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            String key = null;
            Object value = null;
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.KEY)) {
                    jsonParser.nextToken();
                    key = jsonParser.getText();
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    value = deserializationContext.readValue(jsonParser, Object.class);
                }
            }

            return new DetachedProperty<>(key, value);
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class PathJacksonDeserializer extends StdDeserializer<Path> {
        private static final JavaType setType = TypeFactory.defaultInstance().constructCollectionType(HashSet.class, String.class);

        public PathJacksonDeserializer() {
            super(Path.class);
        }

        @Override
        public Path deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Path p = MutablePath.make();

            List<Object> labels = new ArrayList<>();
            List<Object> objects = new ArrayList<>();
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.LABELS)) {
                    jsonParser.nextToken();
                    labels = deserializationContext.readValue(jsonParser, List.class);
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.OBJECTS)) {
                    jsonParser.nextToken();
                    objects = deserializationContext.readValue(jsonParser, List.class);
                }
            }

            for (int i = 0; i < objects.size(); i++) {
                p.extend(objects.get(i), (Set<String>) labels.get(i));
            }

            return p;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class VertexPropertyJacksonDeserializer extends StdDeserializer<VertexProperty> {
        private static final JavaType propertiesType = TypeFactory.defaultInstance().constructMapType(HashMap.class, String.class, Object.class);

        protected VertexPropertyJacksonDeserializer() {
            super(VertexProperty.class);
        }

        @Override
        public VertexProperty deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final DetachedVertexProperty.Builder vp = DetachedVertexProperty.build();

            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.ID)) {
                    jsonParser.nextToken();
                    vp.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.LABEL)) {
                    jsonParser.nextToken();
                    vp.setLabel(jsonParser.getText());
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.VALUE)) {
                    jsonParser.nextToken();
                    vp.setValue(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.PROPERTIES)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        final String key = jsonParser.getCurrentName();
                        jsonParser.nextToken();
                        final Object val = deserializationContext.readValue(jsonParser, Object.class);
                        vp.addProperty(new DetachedProperty(key, val));
                    }
                }
            }

            return vp.create();
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class MetricsJacksonDeserializer extends StdDeserializer<Metrics> {
        public MetricsJacksonDeserializer() {
            super(Metrics.class);
        }

        @Override
        public Metrics deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<String, Object> metricsData = deserializationContext.readValue(jsonParser, Map.class);
            final MutableMetrics m = new MutableMetrics((String)metricsData.get(GraphSONTokens.ID), (String)metricsData.get(GraphSONTokens.NAME));

            m.setDuration(Math.round((Double) metricsData.get(GraphSONTokens.DURATION) * 1000000), TimeUnit.NANOSECONDS);
            for (Map.Entry<String, Long> count : ((Map<String, Long>)metricsData.getOrDefault(GraphSONTokens.COUNTS, new LinkedHashMap<>(0))).entrySet()) {
                m.setCount(count.getKey(), count.getValue());
            }
            for (Map.Entry<String, Long> count : ((Map<String, Long>) metricsData.getOrDefault(GraphSONTokens.ANNOTATIONS, new LinkedHashMap<>(0))).entrySet()) {
                m.setAnnotation(count.getKey(), count.getValue());
            }
            for (MutableMetrics nested : (List<MutableMetrics>)metricsData.getOrDefault(GraphSONTokens.METRICS, new ArrayList<>(0))) {
                m.addNested(nested);
            }
            return m;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class TraversalMetricsJacksonDeserializer extends StdDeserializer<TraversalMetrics> {

        public TraversalMetricsJacksonDeserializer() {
            super(TraversalMetrics.class);
        }

        @Override
        public TraversalMetrics deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final Map<String, Object> traversalMetricsData = deserializationContext.readValue(jsonParser, Map.class);

            return new DefaultTraversalMetrics(
                    Math.round((Double) traversalMetricsData.get(GraphSONTokens.DURATION) * 1000000),
                    (List<MutableMetrics>) traversalMetricsData.get(GraphSONTokens.METRICS)
            );
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class TreeJacksonDeserializer extends StdDeserializer<Tree> {

        public TreeJacksonDeserializer() {
            super(Tree.class);
        }

        @Override
        public Tree deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            final List<Map> data = deserializationContext.readValue(jsonParser, List.class);
            final Tree t = new Tree();
            for (Map<String, Object> entry : data) {
                t.put(entry.get(GraphSONTokens.KEY), entry.get(GraphSONTokens.VALUE));
            }
            return t;
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class IntegerJackonsDeserializer extends StdDeserializer<Integer> {

        protected IntegerJackonsDeserializer() {
            super(Integer.class);
        }

        @Override
        public Integer deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            return jsonParser.getIntValue();
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    static class DoubleJackonsDeserializer extends StdDeserializer<Double> {

        protected DoubleJackonsDeserializer() {
            super(Double.class);
        }

        @Override
        public Double deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            return jsonParser.getDoubleValue();
        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }
}