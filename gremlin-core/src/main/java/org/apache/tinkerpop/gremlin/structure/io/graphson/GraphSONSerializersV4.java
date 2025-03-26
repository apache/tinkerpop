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
import org.apache.tinkerpop.gremlin.process.traversal.util.ImmutableExplanation;
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
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdKeySerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.type.TypeFactory;
import org.javatuples.Pair;
import org.javatuples.Triplet;

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

/**
 * GraphSON serializers for graph-based objects such as vertices, edges, properties, and paths. These serializers
 * present a generalized way to serialize the implementations of core interfaces.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GraphSONSerializersV4 {

    private GraphSONSerializersV4() {
    }

    ////////////////////////////// SERIALIZERS /////////////////////////////////

    final static class VertexJacksonSerializer extends StdScalarSerializer<Vertex> {

        private final boolean normalize;
        private final TypeInfo typeInfo;

        public VertexJacksonSerializer(final boolean normalize, final TypeInfo typeInfo) {
            super(Vertex.class);
            this.normalize = normalize;
            this.typeInfo = typeInfo;
        }

        @Override
        public void serialize(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, vertex.id());
            writeLabel(jsonGenerator, GraphSONTokens.LABEL, vertex.label());
            writeTypeForGraphObjectIfUntyped(jsonGenerator, typeInfo, GraphSONTokens.VERTEX);
            writeProperties(vertex, jsonGenerator, serializerProvider);

            jsonGenerator.writeEndObject();

        }

        private void writeProperties(final Vertex vertex, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider) throws IOException {
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
                        // if you writeObject the property directly it treats it as a standalone VertexProperty which
                        // will write the label duplicating it. we really only want that for embedded types
                        if (typeInfo == TypeInfo.NO_TYPES) {
                            VertexPropertyJacksonSerializer.writeVertexProperty(vertexProperties.next(), jsonGenerator,
                                    serializerProvider, normalize, false);
                        } else {
                            jsonGenerator.writeObject(vertexProperties.next());
                        }
                    }
                    jsonGenerator.writeEndArray();
                }
            }

            jsonGenerator.writeEndObject();
        }
    }

    final static class EdgeJacksonSerializer extends StdScalarSerializer<Edge> {

        private final boolean normalize;

        private final TypeInfo typeInfo;

        public EdgeJacksonSerializer(final boolean normalize, final TypeInfo typeInfo) {
            super(Edge.class);
            this.normalize = normalize;
            this.typeInfo = typeInfo;
        }


        @Override
        public void serialize(final Edge edge, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, edge.id());
            writeLabel(jsonGenerator, GraphSONTokens.LABEL, edge.label());
            writeTypeForGraphObjectIfUntyped(jsonGenerator, typeInfo, GraphSONTokens.EDGE);
            writeVertex(GraphSONTokens.IN, edge.inVertex(), jsonGenerator);
            writeVertex(GraphSONTokens.OUT, edge.outVertex(), jsonGenerator);
            writeProperties(edge, jsonGenerator);

            jsonGenerator.writeEndObject();
        }

        private static void writeVertex(final String vertexDirection, final Vertex v, final JsonGenerator jsonGenerator)
                throws IOException {
            jsonGenerator.writeFieldName(vertexDirection);
            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField(GraphSONTokens.ID, v.id());
            writeLabel(jsonGenerator, GraphSONTokens.LABEL, v.label());
            jsonGenerator.writeEndObject();
        }

        private void writeProperties(final Edge edge, final JsonGenerator jsonGenerator) throws IOException {
            final Iterator<Property<Object>> elementProperties = normalize ?
                    IteratorUtils.list(edge.properties(), Comparators.PROPERTY_COMPARATOR).iterator() : edge.properties();

            if (elementProperties.hasNext()) {
                jsonGenerator.writeFieldName(GraphSONTokens.PROPERTIES);
                jsonGenerator.writeStartObject();

                while (elementProperties.hasNext()) {
                    final Property prop = elementProperties.next();
                    jsonGenerator.writeFieldName(prop.key());
                    jsonGenerator.writeStartArray();

                    if (typeInfo == TypeInfo.NO_TYPES) {
                        jsonGenerator.writeObject(prop.value());
                    } else {
                        jsonGenerator.writeObject(prop);
                    }

                    jsonGenerator.writeEndArray();
                }
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
            writeVertexProperty(property, jsonGenerator, serializerProvider, normalize, includeLabel);
        }

        private static void writeVertexProperty(final VertexProperty property, final JsonGenerator jsonGenerator,
                                                final SerializerProvider serializerProvider, final boolean normalize,
                                                final boolean includeLabel)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeObjectField(GraphSONTokens.ID, property.id());
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, property.value());
            if (includeLabel) {
                writeLabel(jsonGenerator, GraphSONTokens.LABEL, property.label());
            }
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
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        v.setLabel(jsonParser.getText());
                    }
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
            while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                if (jsonParser.getCurrentName().equals(GraphSONTokens.ID)) {
                    jsonParser.nextToken();
                    e.setId(deserializationContext.readValue(jsonParser, Object.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.LABEL)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        e.setLabel(jsonParser.getText());
                    }
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.OUT)) {
                    jsonParser.nextToken();
                    e.setOutV((DetachedVertex) deserializationContext.readValue(jsonParser, Vertex.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.IN)) {
                    jsonParser.nextToken();
                    e.setInV((DetachedVertex) deserializationContext.readValue(jsonParser, Vertex.class));
                } else if (jsonParser.getCurrentName().equals(GraphSONTokens.PROPERTIES)) {
                    jsonParser.nextToken();
                    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
                        jsonParser.nextToken();
                        while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                            e.addProperty(deserializationContext.readValue(jsonParser, Property.class));
                        }
                    }
                }
            }

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
                    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
                        vp.setLabel(jsonParser.getText());
                    }
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

    static class DoubleJacksonDeserializer extends StdDeserializer<Double> {

        protected DoubleJacksonDeserializer() {
            super(Double.class);
        }

        @Override
        public Double deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            if (jsonParser.getCurrentToken().isNumeric())
                return jsonParser.getDoubleValue();
            else  {
                final String numberText = jsonParser.getValueAsString();
                if ("NaN".equalsIgnoreCase(numberText))
                    return Double.NaN;
                else if ("-Infinity".equals(numberText) || "-INF".equalsIgnoreCase(numberText))
                    return Double.NEGATIVE_INFINITY;
                else if ("Infinity".equals(numberText) || "INF".equals(numberText))
                    return Double.POSITIVE_INFINITY;
                else
                    throw new IllegalStateException("Double value unexpected: " + numberText);
            }

        }

        @Override
        public boolean isCachable() {
            return true;
        }
    }

    /**
     * When doing untyped serialization graph objects get a special "type" field appended.
     */
    private static void writeTypeForGraphObjectIfUntyped(final JsonGenerator jsonGenerator, final TypeInfo typeInfo,
                                                         final String type) throws IOException {
        if (typeInfo == TypeInfo.NO_TYPES) {
            jsonGenerator.writeStringField(GraphSONTokens.TYPE, type);
        }
    }

    /**
     * Helper method for writing a label. Starting in v4, the label is an array of String that is inside an object. Only
     * writes the array portion; that is, it assumes the object has already started.
     */
    private static void writeLabel(final JsonGenerator jsonGenerator, final String labelName, final String labelValue) throws IOException {
        jsonGenerator.writeFieldName(labelName);
        jsonGenerator.writeStartArray();
        jsonGenerator.writeString(labelValue);
        jsonGenerator.writeEndArray();
    }
}